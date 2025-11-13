#!/usr/bin/env python3
import os
import gzip
import socket
import logging
from datetime import datetime, timedelta
from pathlib import Path

# 腾讯云 COS SDK
from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client

# 配置
LOG_DIR = Path("/root/ad-click-sender/logs")
HOSTNAME = socket.gethostname()

# 日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# COS 映射
REGION_MAP = {
    "de": "eu-frankfurt",
    "sg": "ap-singapore",
    "us": "na-ashburn"
}
BUCKET_NAME_TEMPLATE = "pando-adx-{region}-1374116111"

def get_cos_client():
    secret_id = os.getenv("SECRET_ID")
    secret_key = os.getenv("SECRET_KEY")
    region_short = os.getenv("REGION")

    if not all([secret_id, secret_key, region_short]):
        raise EnvironmentError("Missing env: SECRET_ID, SECRET_KEY, REGION")
    if region_short not in REGION_MAP:
        raise ValueError(f"Invalid REGION: {region_short}")

    cos_region = REGION_MAP[region_short]
    bucket_name = BUCKET_NAME_TEMPLATE.format(region=region_short)

    config = CosConfig(
        Region=cos_region,
        SecretId=secret_id,
        SecretKey=secret_key,
        Scheme='https'
    )
    client = CosS3Client(config)
    return client, bucket_name

def main():
    # 使用本地时间（假设系统时区为 Asia/Shanghai）
    now_local = datetime.now()  # 依赖系统时区
    t_minus_2 = now_local - timedelta(minutes=2)
    ts_str = t_minus_2.strftime("%Y%m%d%H%M")
    log_filename = f"click.log.{ts_str}"
    log_path = LOG_DIR / log_filename

    if not log_path.exists():
        logger.info(f"[T-2={ts_str}] Log file not found: {log_path}")
        return

    # 压缩并重命名：加上 hostname
    gz_filename = f"{log_filename}.{HOSTNAME}.gz"
    gz_path = LOG_DIR / gz_filename

    try:
        with open(log_path, 'rb') as f_in:
            with gzip.open(gz_path, 'wb') as f_out:
                f_out.writelines(f_in)
        logger.info(f"Compressed: {gz_path}")
    except Exception as e:
        logger.error(f"Compression failed for {log_path}: {e}")
        return

    # COS 目录结构：按日志时间（东八区）组织
    cos_dir = t_minus_2.strftime("%Y/%m/%d/%H/%M")
    cos_key = f"click_logs/{cos_dir}/{gz_filename}"

    try:
        client, bucket = get_cos_client()
        client.upload_file(
            Bucket=bucket,
            LocalFilePath=str(gz_path),
            Key=cos_key
        )
        logger.info(f"Uploaded to COS: {bucket}/{cos_key}")
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        return

    # 清理本地文件
    try:
        gz_path.unlink()
        logger.info(f"Cleaned up: {gz_path}")
    except Exception as e:
        logger.warning(f"Cleanup failed: {e}")

if __name__ == "__main__":
    main()