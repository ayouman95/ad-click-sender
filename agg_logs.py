#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
聚合日志并写入 Elasticsearch 索引 click_records_data
支持无认证（测试集群）和有认证（生产）两种模式
"""

import os
import json
from datetime import datetime, timedelta
from collections import defaultdict
import glob
from pathlib import Path
from elasticsearch import Elasticsearch, helpers

# ==================== 配置 ====================
LOG_DIR = "./logs"
LOG_PREFIX = "click.log."
DEBUG_LOG = "agg_debug.log"

# Elasticsearch 配置
ES_HOST = os.getenv("ES_HOST")
ES_USER = os.getenv("ES_NAME")
ES_PASS = os.getenv("ES_PASS")
ES_INDEX = "click_records_data"  # 测试索引

if not ES_HOST:
    raise EnvironmentError("请设置环境变量：ES_HOST")

# 固定字段
CHANNEL_ID_NUM = 999
CHANNEL = "seed"

def log(msg):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {msg}")
    with open(DEBUG_LOG, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {msg}\n")

def parse_datetime_from_filename(filename: str):
    basename = Path(filename).name
    if not basename.startswith(LOG_PREFIX):
        return None
    dt_str = basename[len(LOG_PREFIX):]
    if not (len(dt_str) == 12 and dt_str.isdigit()):
        return None
    try:
        year, month, day, hour, minute = (
            int(dt_str[:4]),
            int(dt_str[4:6]),
            int(dt_str[6:8]),
            int(dt_str[8:10]),
            int(dt_str[10:12]),
        )
        return datetime(year, month, day, hour, minute)
    except Exception:
        return None

def find_log_files(log_dir, prefix):
    now = datetime.now() - timedelta(minutes=1)
    last_now = now - timedelta(minutes=1)
    pattern = os.path.join(log_dir, f"{prefix}*")
    matched_files = []
    for file in glob.glob(pattern):
        dt = parse_datetime_from_filename(file)
        if dt and last_now < dt <= now:
            matched_files.append(file)
    return sorted(matched_files)

def extract_time(timestamp_str: str) -> str:
    """截断到分钟，格式: yyyy-MM-dd HH:mm:ss"""
    return timestamp_str.split(".")[0][:-2] + "00"

def create_es_client():
    """创建 Elasticsearch 客户端，根据环境变量决定是否使用认证"""
    try:
        # 判断是否提供用户名密码
        if ES_USER is not None and ES_PASS is not None:
            auth = (ES_USER, ES_PASS)
            log("🔐 使用账号密码连接 ES")
            es = Elasticsearch(
                hosts=[ES_HOST],
                basic_auth=auth,
                request_timeout=30,
                verify_certs=True,
                max_retries=3,
                retry_on_timeout=True,
            )
        else:
            log("🔓 连接无认证的测试 ES 集群")
            es = Elasticsearch(
                hosts=[ES_HOST],
                request_timeout=30,
                verify_certs=True,
                max_retries=3,
                retry_on_timeout=True,
            )

        if not es.ping():
            raise ConnectionError("❌ 无法连接到 Elasticsearch")
        log("✅ 成功连接到 Elasticsearch")
        return es
    except Exception as e:
        log(f"❌ 连接 ES 失败: {e}")
        raise

def process_log_file(filepath: str, aggregator):
    open_func = open
    mode = "r"
    if filepath.endswith(".gz"):
        import gzip
        open_func = gzip.open
        mode = "rt"

    try:
        with open_func(filepath, mode, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    continue

                if data.get("statusCode") != 200 and data.get("statusCode") != 301:
                    continue

                touch_type = data.get("touchType", "").lower()
                if touch_type not in ("click", "impression"):
                    continue

                raw_time = data.get("time") or data.get("sendTime")
                if not raw_time:
                    continue
                time_key = extract_time(raw_time)

                offer_id_raw = data.get("offerId", "N/A")
                try:
                    offer_id_int = int(offer_id_raw)
                except (TypeError, ValueError):
                    offer_id_int = 0

                key = (
                    time_key,
                    offer_id_int,
                    data.get("channelId", "N/A"),
                    data.get("siteId", "N/A"),
                    data.get("os", "N/A"),
                    data.get("advertiser", "N/A"),
                    data.get("om", "N/A"),
                    data.get("am", "N/A"),
                    data.get("appId", "N/A"),
                    data.get("pid", "N/A"),
                    data.get("geo", "N/A"),
                )

                if key not in aggregator:
                    aggregator[key] = {"clickCount": 0, "impressionCount": 0}

                if touch_type == "click":
                    aggregator[key]["clickCount"] += 1
                elif touch_type == "impression":
                    aggregator[key]["impressionCount"] += 1
    except Exception as e:
        log(f"❌ 处理文件失败 {filepath}: {e}")

def send_to_es(es, index_name, data_list):
    """批量写入 Elasticsearch"""
    if not data_list:
        log("🟡 无数据可写入 ES")
        return 0, 0

    actions = [
        {
            "_index": index_name,
            "_source": record,
        }
        for record in data_list
    ]

    try:
        success, failed = helpers.bulk(
            es, actions, raise_on_error=False, ignore_status=[400, 409]
        )
        log(f"✅ 成功写入 ES: {success} 条")
        if failed:
            log(f"⚠️  写入失败: {len(failed)} 条")
        return success, failed
    except Exception as e:
        log(f"❌ 批量写入 ES 异常: {e}")
        return 0, len(data_list)

def main():
    log("启动日志聚合与 ES 写入任务")

    if not os.path.exists(LOG_DIR):
        log(f"❌ 目录不存在: {LOG_DIR}")
        return

    log_files = find_log_files(LOG_DIR, LOG_PREFIX)
    if not log_files:
        log("❌ 无匹配日志文件")
        return

    log(f"✅ 发现 {len(log_files)} 个待处理文件")

    aggregator = {}
    for file_path in log_files:
        log(f"处理文件: {file_path}")
        process_log_file(file_path, aggregator)

    output_records = []
    for key, counts in sorted(aggregator.items()):
        time_val, offer_id_int, channel_id, site_id, os_val, advertiser, om, am, app_id, pid, geo = key
        # time_val(yyyy-MM-dd HH:mm:ss格式) 转成时间戳
        dt = int(datetime.strptime(time_val, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
        record = {
            "time": dt,
            "offerId": offer_id_int,
            "offerIdStr": str(offer_id_int),
            "channelId": channel_id,
            "channelIdNum": CHANNEL_ID_NUM,
            "siteId": site_id,
            "os": os_val,
            "advertiser": advertiser,
            "om": om,
            "am": am,
            "appId": app_id,
            "pid": pid,
            "geo": geo,
            "channel": CHANNEL,
            "clickCount": counts["clickCount"],
            "impressionCount": counts["impressionCount"],
        }
        log(f"写入 ES: {record}")
        output_records.append(record)

    total = len(output_records)
    log(f"✅ 聚合完成，准备写入 ES: {total} 条记录")

    # 写入 ES
    try:
        es = create_es_client()
        success, failed = send_to_es(es, ES_INDEX, output_records)
        log(f"🎉 任务完成！写入成功: {success}, 失败: {failed}")
    except Exception as e:
        log(f"❌ 任务失败: {e}")
        raise

if __name__ == "__main__":
    main()