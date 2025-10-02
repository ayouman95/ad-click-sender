package main

import (
	"expvar"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/valyala/fastjson"
)

// -------------------------------
// 数据结构
// -------------------------------

type UDB struct {
	Brand     string `json:"brand"`
	Model     string `json:"model"`
	Publisher string `json:"publisher"`
	Bundle    string `json:"bundle"`
	IP        string `json:"ip"`
	UA        string `json:"ua"`
	Lang      string `json:"lang"`
	OSVersion string `json:"osVersion"`
	GAID      string `json:"gaid"`
	IDFA      string `json:"idfa"`
}

type RawClickData struct {
	OfferID   int64  `json:"offerId"`
	SiteID    int64  `json:"siteId"`
	TouchType string `json:"touchType"`
	Tracking  string `json:"tracking"`
	Cname     string `json:"cname"`
	OS        string `json:"os"`
	UDBs      []UDB  `json:"udbs"`
}

// 展开后的待发送请求
type ClickRequest struct {
	OfferID   int64
	SiteID    int64
	TouchType string
	Tracking  string
	Cname     string
	OS        string
	ClickID   string
	RecvTime  string

	// 从 udb 提取的字段（用于替换）
	UA     string
	IP     string
	Lang   string
	GAID   string
	IDFA   string
	Bundle string
}

type LogEntry struct {
	*ClickRequest
	StatusCode   int
	SendTime     int64
	CompleteTime int64
}

// -------------------------------
// 全局变量
// -------------------------------

var (
	// 环形缓冲区：每分钟一个 slot，共 60个
	bufferRing     [60][]ClickRequest
	bufferRingMu   sync.Mutex // 仅用于分钟切换
	currentSlot    int32      // 原子操作
	logDir         = "./logs"
	expMetrics     = expvar.NewMap("click_sender")
	parsers        sync.Pool
	client         = &http.Client{Timeout: 100 * time.Microsecond}
	logWriterQueue = make(chan *LogEntry, 100000) // 异步日志队列
	shutdown       = make(chan struct{})
	currentLink    = "click.log" // 软链接名
	node           *snowflake.Node
)

const (
	MaxQPS           = 20000
	BatchSize        = MaxQPS
	MaxConns         = 50000
	MaxIdleConns     = 10000
	KeepAlive        = 90 * time.Second
	TLSHandshake     = 10 * time.Second
	ExpectContinue   = 1 * time.Second
	ChannelId        = "999"
	DdjClickIdPrefix = "pdd"
)

func init() {
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Fatal("无法创建日志目录:", err)
	}

	parsers.New = func() interface{} { return new(fastjson.Parser) }

	// 高性能 HTTP 客户端
	transport := &http.Transport{
		MaxIdleConns:          MaxIdleConns,
		MaxIdleConnsPerHost:   1000,
		MaxConnsPerHost:       MaxConns,
		IdleConnTimeout:       KeepAlive,
		TLSHandshakeTimeout:   TLSHandshake,
		ExpectContinueTimeout: ExpectContinue,
		ForceAttemptHTTP2:     true,
	}

	client = &http.Client{
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Timeout: 15 * time.Second,
	}

	// 启动日志写入协程
	go logWriter()

	// 初始化 expvar 指标
	expMetrics.Add("received", 0)
	expMetrics.Add("sent", 0)
	expMetrics.Add("failed", 0)
	expMetrics.Add("dropped", 0)

	workerID := getWorkerID()
	fmt.Printf("Using worker ID: %d\n", workerID)

	var err error
	node, err = snowflake.NewNode(workerID)
	if err != nil {
		panic(fmt.Sprintf("无法创建 Snowflake 节点: %v", err))
	}
}

func init() {

}

// -------------------------------
// 接收点击接口
// -------------------------------

func handleReceiveClick(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "读取失败", http.StatusBadRequest)
		return
	}
	r.Body.Close()

	var p = parsers.Get().(*fastjson.Parser)
	defer parsers.Put(p)

	v, err := p.Parse(string(body))
	if err != nil {
		http.Error(w, "JSON解析失败", http.StatusBadRequest)
		return
	}

	var raw RawClickData
	raw.OfferID = v.GetInt64("offerId")
	raw.SiteID = v.GetInt64("siteId")
	raw.TouchType = string(v.GetStringBytes("touchType"))
	raw.Tracking = string(v.GetStringBytes("tracking"))
	raw.Cname = string(v.GetStringBytes("cname"))
	raw.OS = string(v.GetStringBytes("os"))

	// 解析 udbs 数组
	udbsV := v.GetArray("udbs")
	for _, u := range udbsV {
		udb := UDB{
			Brand:     string(u.GetStringBytes("brand")),
			Model:     string(u.GetStringBytes("model")),
			Publisher: string(u.GetStringBytes("publisher")),
			Bundle:    string(u.GetStringBytes("bundle")),
			IP:        string(u.GetStringBytes("ip")),
			UA:        string(u.GetStringBytes("ua")),
			Lang:      string(u.GetStringBytes("lang")),
			OSVersion: string(u.GetStringBytes("osVersion")),
			GAID:      string(u.GetStringBytes("gaid")),
			IDFA:      string(u.GetStringBytes("idfa")),
		}
		raw.UDBs = append(raw.UDBs, udb)
	}

	if raw.Tracking == "" || len(raw.UDBs) == 0 {
		http.Error(w, "缺少tracking或udbs为空", http.StatusBadRequest)
		return
	}

	// 👉 展开：每个 udb 生成一个 ClickRequest
	var requests []ClickRequest
	requestTime := time.Now()
	requestTimeStr := requestTime.Format("2006-01-02 15:04:05")
	for _, udb := range raw.UDBs {
		clickID := fastGenerateClickID(raw.OfferID)

		req := ClickRequest{
			OfferID:   raw.OfferID,
			SiteID:    raw.SiteID,
			TouchType: raw.TouchType,
			Tracking:  raw.Tracking,
			Cname:     raw.Cname,
			OS:        raw.OS,
			ClickID:   clickID,
			RecvTime:  requestTimeStr,

			// 从 udb 提取
			UA:     udb.UA,
			IP:     udb.IP,
			Lang:   udb.Lang,
			GAID:   udb.GAID,
			IDFA:   udb.IDFA,
			Bundle: udb.Bundle,
		}

		trackingReplaced := replaceTracking(&req)
		req.Tracking = trackingReplaced

		requests = append(requests, req)
	}

	// 写入环形缓冲区（每分钟一个 slot）

	slot := int(atomic.LoadInt32(&currentSlot))
	bufferRingMu.Lock()
	bufferRing[slot] = append(bufferRing[slot], requests...)
	bufferRingMu.Unlock()

	expMetrics.Add("received", int64(len(requests)))

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf(`{"status":"ok","clicks":%d}`, len(requests))))
}

// -------------------------------
// 快速生成 click_id（避免 uuid/md5）
// -------------------------------

var clickIDCounter uint64

func fastGenerateClickID(offerID int64) string {
	return fmt.Sprintf("%s_%s%s", strconv.FormatInt(offerID, 10), DdjClickIdPrefix, node.Generate().String())
}

// -------------------------------
// 每分钟调度器
// -------------------------------

func scheduler() {
	now := time.Now()
	next := now.Truncate(time.Minute).Add(time.Minute) // 下一个整分钟，如 10:01:00
	time.Sleep(time.Until(next))
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			log.Println("开始处理批次")
			thisSlotTime := time.Now()
			lastSlotTime := thisSlotTime.Add(-1 * time.Minute)
			thisSlot := int(thisSlotTime.Minute() % 60)
			lastSlot := int(lastSlotTime.Minute() % 60)
			atomic.StoreInt32(&currentSlot, int32(thisSlot))

			// 获取上一分钟的数据
			bufferRingMu.Lock()
			batch := bufferRing[lastSlot]
			bufferRing[lastSlot] = nil // 释放内存
			bufferRingMu.Unlock()

			if len(batch) == 0 {
				continue
			}

			go sendBatch(batch, now)
		case <-shutdown:
			return
		}
	}
}

// -------------------------------
// 发送批次（每秒 2万 均匀发送）
// -------------------------------

func sendBatch(batch []ClickRequest, minute time.Time) {
	start := time.Now()
	total := len(batch)
	if total == 0 {
		return
	}

	log.Printf("开始发送批次: %d 个请求", total)

	sem := make(chan struct{}, runtime.GOMAXPROCS(0)*100) // 并发控制
	var sent, failed int64

	interval := time.Second / time.Duration(BatchSize)
	if interval < 50*time.Microsecond {
		interval = 50 * time.Microsecond
	}

	var wg sync.WaitGroup

	for i := 0; i < total; i++ {
		// 主 goroutine 控制节奏
		next := start.Add(time.Duration(i+1) * interval)
		if d := time.Until(next); d > 0 {
			time.Sleep(d)
		}

		select {
		case sem <- struct{}{}:
		default:
			expMetrics.Add("dropped", int64(1))
			break
		}

		wg.Add(1)

		go func(cd ClickRequest) {
			defer wg.Done()
			defer func() { <-sem }()
			sendTime := time.Now()
			url := cd.Tracking
			// 设置header
			reqHeaders := make(http.Header)
			reqHeaders.Set("User-Agent", cd.UA)
			reqHeaders.Set("X-Forwarded-For", cd.IP)
			reqHeaders.Set("Accept-Language", "en-US;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6")

			req, _ := http.NewRequest("GET", url, nil)
			resp, err := client.Do(req)
			status := 0
			if err != nil {
				status = -1
				atomic.AddInt64(&failed, 1)
				expMetrics.Add("failed", 1)
			} else {
				status = resp.StatusCode
				defer resp.Body.Close()
			}
			completeTime := time.Now()
			// 发送到日志队列
			logWriterQueue <- &LogEntry{
				ClickRequest: &cd,
				StatusCode:   status,
				SendTime:     sendTime.UnixNano(),
				CompleteTime: completeTime.UnixNano(),
			}

			atomic.AddInt64(&sent, 1)
			expMetrics.Add("sent", 1)

		}(batch[i])
	}

	// 这里等的其实是最后一批 基本上等于一分钟结束
	wg.Wait()
	log.Printf("批次完成: sent=%d, failed=%d", sent, failed)
}

// -------------------------------
// 替换 tracking 占位符（高性能字符串拼接）
// -------------------------------

func replaceTracking(req *ClickRequest) string {
	u := req.Tracking
	u = strings.ReplaceAll(u, "{siteid}", strconv.FormatInt(req.SiteID, 10))
	u = strings.ReplaceAll(u, "{offer_id}", strconv.FormatInt(req.OfferID, 10))
	u = strings.ReplaceAll(u, "{click_id}", req.ClickID)
	u = strings.ReplaceAll(u, "{channel}", ChannelId)

	u = strings.ReplaceAll(u, "{ip}", req.IP)
	u = strings.ReplaceAll(u, "{lang}", req.Lang)
	u = strings.ReplaceAll(u, "{ua}", req.UA)
	u = strings.ReplaceAll(u, "{c_id}", req.Cname)
	u = strings.ReplaceAll(u, "{c}", req.Cname)
	u = strings.ReplaceAll(u, "{bundle}", req.Bundle)
	u = strings.ReplaceAll(u, "{publisher}", req.Bundle)
	u = strings.ReplaceAll(u, "{brand}", req.Bundle)
	u = strings.ReplaceAll(u, "{model}", req.Bundle)
	u = strings.ReplaceAll(u, "{is_retargeting}", "false")

	if strings.EqualFold("android", req.OS) {
		u = strings.ReplaceAll(u, "{gaid}", req.GAID)
		u = strings.ReplaceAll(u, "{idfa}", "")
	} else if strings.EqualFold("ios", req.OS) {
		u = strings.ReplaceAll(u, "{gaid}", "")
		u = strings.ReplaceAll(u, "{idfa}", req.IDFA)
	}
	return u
}

// -------------------------------
// 异步日志写入协程
// -------------------------------

func logWriter() {
	var bufPool = sync.Pool{New: func() interface{} { return new(strings.Builder) }}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case entry := <-logWriterQueue:
			minute := time.Unix(0, entry.SendTime).Format("200601021504")
			filename := filepath.Join(logDir, "click.log."+minute)

			// 获取 builder
			sb := bufPool.Get().(*strings.Builder)
			sb.Reset()

			// 格式话entry.SendTime成带毫秒的形式
			sendTimeStr := time.Unix(0, entry.SendTime).Format("2006-01-02 15:04:05.000")
			completeTimeStr := time.Unix(0, entry.CompleteTime).Format("2006-01-02 15:04:05.000")
			// 拼接日志行
			sb.WriteString(fmt.Sprintf(
				`{"offerId":%d,"siteId":%d,"touchType":"%s","tracking":"%s","cname":"%s","os":"%s","clickId":"%s","statusCode":%d,"sendTime":"%s","completeTime":"%s"}`,
				entry.OfferID, entry.SiteID, entry.TouchType, entry.Tracking,
				entry.Cname, entry.OS, entry.ClickID,
				entry.StatusCode, sendTimeStr, completeTimeStr))
			sb.WriteString("\n")

			// 写入文件
			file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
			if err == nil {
				file.WriteString(sb.String())
				file.Close()
			}

			bufPool.Put(sb)

		case <-ticker.C:
			// 每分钟清理旧日志
			if time.Now().Second() == 0 {
				//filename := filepath.Join(logDir, "click.log."+time.Now().Format("2006010215"))
				//updateSymlink(filename)
				go cleanupOldLogs()
			}

		case <-shutdown:
			return
		}
	}
}

func updateSymlink(target string) {
	// 先删除旧的软链接
	if err := os.Remove(currentLink); err != nil && !os.IsNotExist(err) {
		log.Printf("删除软链接失败: %v", err)
	}

	// 创建新软链接
	if err := os.Symlink(target, currentLink); err != nil {
		log.Printf("创建软链接失败: %v", err)
	} else {
		log.Printf("软链接更新: %s -> %s", currentLink, target)
	}
}

// -------------------------------
// 清理旧日志
// -------------------------------

func cleanupOldLogs() {
	files, _ := filepath.Glob(filepath.Join(logDir, "click.log.*"))
	if len(files) <= 60 {
		return
	}

	var logs []struct{ name, ts string }
	re := regexp.MustCompile(`click\.log\.(\d{12})$`)
	for _, f := range files {
		m := re.FindStringSubmatch(f)
		if len(m) == 2 {
			logs = append(logs, struct{ name, ts string }{f, m[1]})
		}
	}

	sort.Slice(logs, func(i, j int) bool {
		return logs[i].ts < logs[j].ts
	})

	for i := 0; i < len(logs)-60; i++ {
		os.Remove(logs[i].name)
	}
}

// -------------------------------
// 指标接口
// -------------------------------

func metricsHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	expMetrics.Do(func(kv expvar.KeyValue) {
		fmt.Fprintf(w, "%s: %s\n", kv.Key, kv.Value)
	})
}

// -------------------------------
// 主函数
// -------------------------------

func main() {
	go scheduler()

	http.HandleFunc("/click", handleReceiveClick)
	http.HandleFunc("/metrics", metricsHandler)
	http.HandleFunc("/hc", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	log.Println("服务启动，监听 :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal("启动失败:", err)
	}
}
