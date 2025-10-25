package rta

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/satori/go.uuid"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// 常量定义
const (
	RTA_ZHIKE_AK  = "252433141ffaa954195e5d9aca6f72de"
	RTA_VIKING_AK = "00a3c4d5e32932a69d134e8e4c6c9d88"
	RTA_ZHIKE_SK  = "7a9ffe698e61d04f7fc80dfc8ded52a8"
	RTA_VIKING_SK = "2635e0b04e7483478f43b259b62689d9"

	ROW_URL                = "https://growth-rta.byteintl.com/api/v1/rta/%s"
	US_URL                 = "https://growth-rta.tiktokv-us.com/api/v1/rta/%s"
	EU_URL                 = "https://growth-rta.tiktokv-eu.com/api/v1/rta/%s"
	RTA_VIKING_NETWORK_URL = "http://t.vikingmedia.mobi/api/rest/pub/tiktokRTA?pub_id=934&key=aiosGNUVWYZ01237"
	RTA_VIKING_REPORT_URL  = "http://t.vikingmedia.mobi/api/rest/pub/tiktokRTA/report?pub_id=934&key=aiosGNUVWYZ01237"
	APPID_TT_L             = "com.zhiliaoapp.musically.go"
)

var (
	// 全局复用的 HTTP Client
	httpClient = &http.Client{
		Transport: &http.Transport{
			// 控制最大连接数
			MaxConnsPerHost:     100,              // 每个 host 最大连接数
			MaxIdleConns:        100,              // 最大空闲连接
			MaxIdleConnsPerHost: 32,               // 每个 host 最大空闲连接
			IdleConnTimeout:     60 * time.Second, // 空闲连接超时
			DisableKeepAlives:   false,            // 启用 Keep-Alive
			DisableCompression:  true,             // 禁用压缩（可选）
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Timeout: 1 * time.Second, // 整个请求超时
	}
)

type GeosTimeZone struct {
	TimeZone string `json:"time_zone"`
	Geo      string `json:"c_code"`
}

// Offers 订单对象
type Offers struct {
	// 主键
	Id int64 `json:"id"`

	// 订单名称
	Title string `json:"title"`

	// offer ID，对外id
	OfferId int64 `json:"offerId"`

	// 当前用户的ID
	Creator string `json:"creator"`

	// 广告主ID
	AdvertiserId string `json:"advertiserId"`

	// 广告主投放链接
	AttributionLink string `json:"attributionLink"`

	// 是否在ddj的attribution link中添加referrer参数  0-不添加 1-添加
	AddReferrer int `json:"addReferrer"`

	// app商店链接
	PreviewLink string `json:"previewLink"`

	// offer状态(active, disabled, paused)
	Status string `json:"status"`

	// 广告归因三方平台
	Mmp string `json:"mmp"`

	// 媒体在广告归因三方平台注册的账号
	Pid string `json:"pid"`

	// APP在应用商店上的图标
	AppIcon string `json:"appIcon"`

	// APP在应用商店上的ID
	AppId string `json:"appId"`

	// 广告系列名称
	Cname string `json:"cname"`

	// 是否测试模式
	Test int `json:"test"`

	// 投放广告的标签
	Tag string `json:"tag"`

	// 投放广告的分类
	Vertical string `json:"vertical"`

	// 负责当前offer的商务人员的ID
	OmId string `json:"omId"`

	// 负责当前offer的第二商务人员的ID
	SecondaryOmId string `json:"secondaryOmId"`

	// 负责当前offer的运营人员ID
	AmId string `json:"amId"`

	// 负责当前offer的第二运营人员ID
	SecondaryAmId string `json:"secondaryAmId"`

	// APP的手机系统
	Os string `json:"os"`

	// 投放地区的ID
	GeoId string `json:"geoId"`

	// 广告主投放的要求
	Kpi string `json:"kpi"`

	// 广告主结算周期
	SettlementDay int64 `json:"settlementDay"`

	// 当前offer发送点击请求的最大条数
	ClickCap int64 `json:"clickCap"`

	// 广告主给的预算，通常按转化数结算（如install）
	TotalCap int `json:"totalCap"`

	// 广告主给的预算，通常按转化数结算（如install） daily
	Cap int `json:"cap"`

	// 目标转化率
	TargetCvr float64 `json:"targetCvr"`

	// 自己系统发送请求率
	TargetSeedRate float64 `json:"targetSeedRate"`

	// Site ID 生命周期时间
	SiteIdLifeTime int `json:"siteIdLifeTime"`

	// Site ID 生命周期时间单位（day OR hour）
	TimeUnit string `json:"timeUnit"`

	// site id最大可用值
	MaxInstallsPerSiteId int `json:"maxInstallsPerSiteId"`

	// site id最小可用值
	MinInstallsPerSiteId int `json:"minInstallsPerSiteId"`

	// site id被禁用最小值
	MinDisabledInstallsPerSiteId int `json:"minDisabledInstallsPerSiteId"`

	// site id被禁用install最小值 1h
	MinDisabledInstallsOneHour int `json:"minDisabledInstallsOneHour"`

	// site id被禁用install最小值 6h
	MinDisabledInstallsSixHour int `json:"minDisabledInstallsSixHour"`

	// site id被禁用install最小值 1d
	MinDisabledInstallsOneDay int `json:"minDisabledInstallsOneDay"`

	// site id被禁用fraud rate最小值 1h
	MinDisabledFraudRateOneHour float64 `json:"minDisabledFraudRateOneHour"`

	// site id被禁用fraud rate最小值 6h
	MinDisabledFraudRateSixHour float64 `json:"minDisabledFraudRateSixHour"`

	// site id被禁用fraud rate最小值 1d
	MinDisabledFraudRateOneDay float64 `json:"minDisabledFraudRateOneDay"`

	// 自动过期设置开关('0-打开', '1'-关闭)
	AutoExpirationFlag string `json:"autoExpirationFlag"`

	// 自动过期设置
	AutoExpirationTime time.Time `json:"autoExpirationTime"`

	// 自动激活设置开关('0-打开', '1'-关闭)
	AutoActiveFlag string `json:"autoActiveFlag"`

	// 自动激活设置
	AutoActiveTime time.Time `json:"autoActiveTime"`

	// 混量时同一条数据在此天数内不能重复使用
	ReattributionWindow int `json:"reattributionWindow"`

	// 是否开启混量  0打开 1关闭
	MixSeedStatus int `json:"mixSeedStatus"`

	// 混量vertical
	MixAudience string `json:"mixAudience"`

	// target valid cvr
	TargetValidCvr float64 `json:"targetValidCvr"`

	// 是否开启targetValidCvr
	TargetValidCvrFlag int `json:"targetValidCvrFlag"`

	// siteId mode
	// 1. mix
	// 2. separate
	SiteIdMode string `json:"siteIdMode"`

	// seed install是否产生seedClick  0 产生  1 不产生
	MixSeed int `json:"mixSeed"`

	// 是否启用RTA 0 启用  1 不启用
	AdoptRtaModel int `json:"adoptRtaModel"`

	// 是否异步 0 异步 1 同步  默认0
	SeedAsync int `json:"seedAsync"`

	// 删除标志（0代表存在 2代表删除）
	DelFlag string `json:"delFlag"`

	// 基础实体字段 (继承自 BaseEntity)
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
	CreatedBy string    `json:"createdBy"`
	UpdatedBy string    `json:"updatedBy"`
}

// 数据结构定义
type RTAReqData struct {
	PackageName  string `json:"package_name"`
	Os           string `json:"os"`
	Country      string `json:"country"`
	Gaid         string `json:"gaid"`
	Idfa         string `json:"idfa"`
	ClientIp     string `json:"client_ip"`
	UserAgent    string `json:"user_agent"`
	OsVersion    string `json:"os_version"`
	Model        string `json:"model"`
	Brand        string `json:"brand"`
	Lang         string `json:"lang"`
	MediaSource  string `json:"media_source"`
	Channel      string `json:"channel"`
	BundleId     string `json:"bundle_id"`
	SiteId       string `json:"site_id"`
	CampaignName string `json:"campaign_name"`
	CampaignId   string `json:"campaign_id"`
	AdName       string `json:"ad_name"`
	AdId         string `json:"ad_id"`
}

type AdSize struct {
	Height int `json:"height"`
	Width  int `json:"width"`
}

type GeoCodeProperties struct {
	CountryCode  string `json:"country_code"`
	Subdivision  string `json:"subdivision"`
	LocationCode string `json:"location_code"`
}

type GeoCodeLine struct {
	Properties GeoCodeProperties `json:"properties"`
}

type RTAResponseData struct {
	TargetList []RTATarget `json:"target_list"`
	RequestId  string      `json:"request_id"`
}

type RTATarget struct {
	Target bool `json:"target"`
}

type TiktokRtaResp struct {
	Code int             `json:"code"`
	Data RTAResponseData `json:"data"`
}

type RTAReportData struct {
	AppId           string `json:"appId"`
	LastAdRequestId string `json:"lastAdRequestId"`
	Os              string `json:"os"`
	DeviceId        string `json:"deviceId"`
	BiddingResult   bool   `json:"biddingResult"`
	RtaId           string `json:"rtaId"`
	// ad_info相关参数
	AdId         string `json:"adId"`
	AdName       string `json:"adName"`
	CampaignId   string `json:"campaignId"`
	CampaignName string `json:"campaignName"`
}

type RtaService struct {
	zhikeRtaIdMap        map[string]string
	zhikeRtaIdMapForLite map[string]string
	zhikeAppIdMap        map[string]string
	zhikeAppIdMapForLite map[string]string
	zhikeUrlMap          map[string]string
	geoStatesMap         map[string][]string
	stateCityMap         map[string][]string
	geosTimeZoneMap      map[string][]string
	adTypeList           []string
	adPlacementList      []string
	adSizeMap            map[string][]AdSize
	networkAccessList    []string
	resolutions          []string
}

func NewRtaService() *RtaService {
	service := &RtaService{
		zhikeRtaIdMap: map[string]string{
			"ID": "1", "TH": "2", "BR": "3", "MX": "4", "VN": "5",
			"CA": "6", "MY": "7", "CL": "8", "US": "9", "AU": "10",
			"GB": "11", "DE": "12", "FR": "15", "ES": "16", "JP": "50",
			"KR": "51", "NL": "52", "RO": "54", "KW": "55", "ZA": "56",
			"TR": "57", "QA": "57", "PH": "59", "SA": "60", "AE": "61",
		},
		zhikeRtaIdMapForLite: map[string]string{
			"BR": "103", "VN": "105", "PH": "111", "ID": "114",
		},
		zhikeAppIdMap: map[string]string{
			"ID": "1180", "TH": "1180", "BR": "1233", "MX": "1233", "VN": "1180",
			"CA": "1233", "MY": "1180", "CL": "1233", "US": "1233", "AU": "1233",
			"GB": "1233", "DE": "1233", "FR": "1233", "ES": "1233", "JP": "1180",
			"KR": "1180", "NL": "1233", "RO": "1233", "KW": "1233", "ZA": "1233",
			"TR": "1233", "QA": "1233", "PH": "1180", "SA": "1233", "AE": "1233",
		},
		zhikeAppIdMapForLite: map[string]string{
			"BR": "1340", "PH": "1340", "VN": "1340", "ID": "1340",
		},
		zhikeUrlMap: map[string]string{
			"ID": ROW_URL, "TH": ROW_URL, "BR": ROW_URL, "MX": ROW_URL, "VN": ROW_URL,
			"CA": ROW_URL, "MY": ROW_URL, "CL": ROW_URL, "US": US_URL, "AU": ROW_URL,
			"GB": EU_URL, "DE": EU_URL, "FR": EU_URL, "ES": EU_URL, "JP": ROW_URL,
			"KR": ROW_URL, "NL": EU_URL, "RO": EU_URL, "KW": ROW_URL, "ZA": ROW_URL,
			"TR": ROW_URL, "QA": ROW_URL, "PH": ROW_URL, "SA": ROW_URL, "AE": ROW_URL,
		},
		geoStatesMap:      make(map[string][]string),
		stateCityMap:      make(map[string][]string),
		geosTimeZoneMap:   make(map[string][]string),
		adTypeList:        []string{"banner", "video", "native"},
		adPlacementList:   make([]string, 0),
		adSizeMap:         make(map[string][]AdSize),
		networkAccessList: make([]string, 0),
		resolutions: []string{
			"320x480", "320x400", "480x800", "480x854", "540x960",
			"600x1024", "720x1184", "720x1196", "720x1280", "720x1440",
			// ... 其他分辨率
		},
	}

	// 初始化 adPlacementList
	for i := 0; i < 12; i++ {
		service.adPlacementList = append(service.adPlacementList, strconv.Itoa(i))
	}

	// 初始化 networkAccessList
	for i := 0; i < 8; i++ {
		service.networkAccessList = append(service.networkAccessList, strconv.Itoa(i))
	}

	// 初始化 adSizeMap
	service.initAdSizeMap()

	return service
}

func (s *RtaService) initAdSizeMap() {
	// Banner sizes
	bannerSizes := []AdSize{
		{Height: 90, Width: 640}, {Height: 90, Width: 1000},
		{Height: 90, Width: 960}, {Height: 90, Width: 750},
		{Height: 100, Width: 640}, {Height: 50, Width: 320},
	}

	// Video sizes
	videoSizes := []AdSize{
		{Height: 500, Width: 600}, {Height: 300, Width: 400},
		{Height: 480, Width: 640},
	}

	// Native sizes
	nativeSizes := []AdSize{
		{Height: 627, Width: 1200}, {Height: 300, Width: 600},
		{Height: 720, Width: 1280},
	}

	s.adSizeMap["banner"] = bannerSizes
	s.adSizeMap["video"] = videoSizes
	s.adSizeMap["native"] = nativeSizes
}

func (s *RtaService) Init() error {
	// 加载 geo 数据
	if err := s.loadGeoData(); err != nil {
		return err
	}

	// 加载时区数据（模拟）
	s.loadTimeZoneData()

	return nil
}

func (s *RtaService) loadGeoData() error {
	// 读取资源文件
	absPath, _ := filepath.Abs("unlo-geocoded.json")
	data, err := ioutil.ReadFile(absPath)
	if err != nil {
		return fmt.Errorf("failed to read geo data file: %v", err)
	}

	lines := strings.Split(string(data), "\n")
	rtaGeos := make(map[string]bool)

	// 合并所有国家代码
	for k := range s.zhikeRtaIdMap {
		rtaGeos[k] = true
	}
	for k := range s.zhikeRtaIdMapForLite {
		rtaGeos[k] = true
	}

	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}

		var geoCodeLine GeoCodeLine
		if err := json.Unmarshal([]byte(line), &geoCodeLine); err != nil {
			continue
		}

		if geoCodeLine.Properties.CountryCode == "" {
			continue
		}

		if rtaGeos[geoCodeLine.Properties.CountryCode] {
			geo := geoCodeLine.Properties.CountryCode
			state := geoCodeLine.Properties.Subdivision

			if state != "" {
				s.geoStatesMap[geo] = append(s.geoStatesMap[geo], state)
				city := geoCodeLine.Properties.LocationCode
				if city != "" {
					s.stateCityMap[state] = append(s.stateCityMap[state], city)
				}
			}
		}
	}

	return nil
}

func (s *RtaService) loadTimeZoneData() {
	// 读取当前目录下geos.json文件
	file, err := os.Open("geos.json")
	if err != nil {
		log.Printf("无法打开文件: %v\n", err)
		return
	}
	defer file.Close()

	// 2. 使用 json.Decoder 逐行读取
	decoder := json.NewDecoder(file)

	for {
		var geoTimeZone GeosTimeZone
		err := decoder.Decode(&geoTimeZone)
		if err != nil {
			// 文件结束
			break
		}
		s.geosTimeZoneMap[geoTimeZone.Geo] = append(s.geosTimeZoneMap[geoTimeZone.Geo], geoTimeZone.TimeZone)
	}
}

func (s *RtaService) generateSign(signKeyInfo string, sk string) string {
	key := []byte(sk)
	h := hmac.New(sha256.New, key)
	h.Write([]byte(signKeyInfo))
	return hex.EncodeToString(h.Sum(nil))
}

func sha256HMAC(key []byte, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return []byte(fmt.Sprintf("%x", mac.Sum(nil)))
}

// ak - access key, sk - secret key
func (s *RtaService) sign(ak string, sk string, requestTimestamp int64, body []byte) string {
	expiration := 1800 //有效时间, 单位是s, 根据自己的业务的实际情况调整
	signKeyInfo := fmt.Sprintf("auth-v1/%s/%d/%d", ak, requestTimestamp, expiration)
	signKey := sha256HMAC([]byte(sk), []byte(signKeyInfo))
	signResult := sha256HMAC(signKey, body)
	return fmt.Sprintf("%v/%v", signKeyInfo, string(signResult))
}

func (s *RtaService) CheckRtaZhike(rtaReuestData *RTAReqData) bool {
	return s.checkRtaTT(rtaReuestData, RTA_ZHIKE_AK, RTA_ZHIKE_SK, fmt.Sprintf(s.zhikeUrlMap[rtaReuestData.Country], "network"), fmt.Sprintf(s.zhikeUrlMap[rtaReuestData.Country], "report"))
}

func (s *RtaService) CheckRtaViking(rtaReuestData *RTAReqData) bool {
	return s.checkRtaTT(rtaReuestData, RTA_VIKING_AK, RTA_VIKING_SK, RTA_VIKING_NETWORK_URL, RTA_VIKING_REPORT_URL)
}

func (s *RtaService) checkRtaTT(rtaReqData *RTAReqData, ak, sk, networkUrl, reportUrl string) bool {
	// 构建参数
	paramMap := make(map[string]interface{})

	appId := s.zhikeAppIdMap[rtaReqData.Country]
	if rtaReqData.PackageName == APPID_TT_L { // APPID_TT_L
		appId = s.zhikeAppIdMapForLite[rtaReqData.Country]
	}

	paramMap["app_id"] = appId
	paramMap["country"] = rtaReqData.Country

	// 获取随机州/城市
	state := "unknown"
	if states, ok := s.geoStatesMap[rtaReqData.Country]; ok && len(states) > 0 {
		state = states[rand.Intn(len(states))]
	}
	paramMap["state"] = state

	city := "unknown"
	if cities, ok := s.stateCityMap[state]; ok && len(cities) > 0 {
		city = cities[rand.Intn(len(cities))]
	}
	paramMap["city"] = city

	paramMap["os"] = rtaReqData.Os
	timeStamp := time.Now().Unix()
	paramMap["timestamp"] = timeStamp

	rtaId := s.zhikeRtaIdMap[rtaReqData.Country]
	if rtaReqData.PackageName == APPID_TT_L {
		rtaId = s.zhikeRtaIdMapForLite[rtaReqData.Country]
	}
	paramMap["rta_id_list"] = []string{rtaId}

	// 构建广告信息
	adInfo := make(map[string]string)
	adType := s.adTypeList[rand.Intn(len(s.adTypeList))]
	adPlacement := s.adPlacementList[rand.Intn(len(s.adPlacementList))]
	adWidth := "unknown"
	adHeight := "unknown"

	if sizes, ok := s.adSizeMap[adType]; ok && len(sizes) > 0 {
		size := sizes[rand.Intn(len(sizes))]
		adWidth = strconv.Itoa(size.Width)
		adHeight = strconv.Itoa(size.Height)
	}

	adInfo["ad_type"] = adType
	adInfo["ad_placement"] = adPlacement
	adInfo["ad_width"] = adWidth
	adInfo["ad_height"] = adHeight
	adInfo["ad_name"] = rtaReqData.AdName
	adInfo["ad_id"] = rtaReqData.AdId

	adList := []map[string]string{adInfo}

	campaignsInfo := make(map[string]interface{})
	campaignsInfo["ad_list"] = adList
	campaignsInfo["campaign_name"] = rtaReqData.CampaignName
	campaignsInfo["campaign_id"] = rtaReqData.CampaignId

	campaignsList := []map[string]interface{}{campaignsInfo}
	campaignsJson, _ := json.Marshal(campaignsList)
	paramMap["campaigns_info"] = string(campaignsJson)

	// 设备信息
	adRequestId := generateUUID()
	paramMap["ad_request_id"] = adRequestId
	deviceId := rtaReqData.Gaid

	if strings.ToLower(rtaReqData.Os) == "android" {
		paramMap["gaid"] = rtaReqData.Gaid
		paramMap["android_id"] = rtaReqData.Gaid
		paramMap["idfa"] = ""
	} else {
		deviceId = rtaReqData.Idfa
		paramMap["idfa"] = rtaReqData.Idfa
		paramMap["android_id"] = ""
		paramMap["gaid"] = ""
	}

	paramMap["client_ip"] = rtaReqData.ClientIp
	paramMap["user_agent"] = rtaReqData.UserAgent
	paramMap["os_version"] = rtaReqData.OsVersion
	paramMap["device_model"] = rtaReqData.Model
	paramMap["device_brand"] = rtaReqData.Brand
	paramMap["sys_language"] = rtaReqData.Lang
	paramMap["device_resolution"] = s.resolutions[rand.Intn(len(s.resolutions))]

	// 网络信息
	networkCarrier := "unknown"
	// 这里应该调用 RegionUtils.getCityInfoFull
	cityInfo := searchIp(rtaReqData.ClientIp)
	if cityInfo != "" {
		cityInfoSplit := strings.Split(cityInfo, "|")
		if len(cityInfoSplit) == 5 {
			networkCarrier = cityInfoSplit[4]
		}
	}
	paramMap["network_carrier"] = networkCarrier

	networkAccess := s.networkAccessList[rand.Intn(len(s.networkAccessList))]
	paramMap["network_access"] = networkAccess

	deviceTimeZone := "unknown"
	if timezones, ok := s.geosTimeZoneMap[rtaReqData.Country]; ok && len(timezones) > 0 {
		deviceTimeZone = timezones[rand.Intn(len(timezones))]
	}
	paramMap["device_timezone"] = deviceTimeZone

	// 随机注册时间
	fiveYearsAgo := time.Now().Unix() - 5*365*24*60*60
	oneYearAgo := time.Now().Unix() - 1*365*24*60*60
	registrationTime := rand.Int63n(oneYearAgo-fiveYearsAgo) + fiveYearsAgo
	paramMap["device_network_registration_time"] = strconv.FormatInt(registrationTime, 10)

	paramMap["media_source"] = rtaReqData.MediaSource
	paramMap["channel"] = rtaReqData.Channel
	paramMap["bundle_id"] = rtaReqData.BundleId
	paramMap["site_id"] = rtaReqData.SiteId
	paramMap["site_name"] = rtaReqData.SiteId
	paramMap["campaign_name"] = rtaReqData.CampaignName
	paramMap["campaign_id"] = rtaReqData.CampaignId
	paramMap["ad_name"] = rtaReqData.AdName
	paramMap["ad_id"] = rtaReqData.AdId
	paramMap["package_name"] = rtaReqData.PackageName

	paramMapJson, _ := json.Marshal(paramMap)
	sign := s.sign(ak, sk, timeStamp, paramMapJson)
	// 发送请求
	headers := map[string]string{
		"Content-Type": "application/json",
		"Agw-Js-Conv":  "str",
		"Agw-Auth":     sign,
	}

	// 这里应该发送 HTTP 请求
	resp, err := s.sendRequest(networkUrl, paramMap, headers)
	// 模拟响应处理
	if resp != nil && resp.StatusCode == http.StatusOK {
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)

		var resp *TiktokRtaResp

		if err := json.Unmarshal(body, &resp); err == nil {
			if resp.Code == 0 && len(resp.Data.TargetList) > 0 {
				for _, target := range resp.Data.TargetList {
					if target.Target {
						// report
						rtaReportData := &RTAReportData{
							AppId:           appId,
							LastAdRequestId: adRequestId,
							Os:              rtaReqData.Os,
							DeviceId:        deviceId,
							BiddingResult:   true,
							RtaId:           rtaId,
							AdId:            rtaReqData.AdId,
							AdName:          rtaReqData.AdName,
							CampaignId:      rtaReqData.CampaignId,
							CampaignName:    rtaReqData.CampaignName,
						}

						s.reportRta(rtaReportData, ak, sk, reportUrl)
						return true
					}
				}
			}
		}
	} else {
		log.Println("Rta Error:", err)
	}
	return false
}

func (s *RtaService) reportRta(rtaReportData *RTAReportData, ak string, sk string, reportUrl string) {
	reuestId := generateUUID()
	timestamp := time.Now().Unix()

	// 构建biddingResultAdInfo
	biddingResultAdInfo := map[string]interface{}{
		"ad_id":         rtaReportData.AdId,
		"ad_name":       rtaReportData.AdName,
		"campaign_id":   rtaReportData.CampaignId,
		"campaign_name": rtaReportData.CampaignName,
	}

	// 构建biddingResult
	biddingResult := map[string]interface{}{
		"bidding_result": rtaReportData.BiddingResult,
		"rta_id":         rtaReportData.RtaId,
		"failed_reason":  "",
		"ad_info":        biddingResultAdInfo,
	}

	// 构建biddingResultList
	biddingResultList := []map[string]interface{}{biddingResult}

	// 构建paramMap
	paramMap := map[string]interface{}{
		"app_id":             rtaReportData.AppId,
		"last_ad_request_id": rtaReportData.LastAdRequestId,
		"report_request_id":  reuestId,
		"timestamp":          timestamp,
		"bidding_results":    biddingResultList,
	}
	// 根据操作系统类型设置设备ID
	if rtaReportData.Os == "android" {
		paramMap["gaid"] = rtaReportData.DeviceId
	} else {
		paramMap["gaid"] = rtaReportData.DeviceId
	}

	paramMapJson, _ := json.Marshal(paramMap)
	sign := s.sign(ak, sk, timestamp, paramMapJson)
	headerMap := map[string]string{
		"Content-Type": "application/json",
		"Agw-Js-Conv":  "str",
		"Agw-Auth":     sign,
	}

	_, err := s.sendRequest(reportUrl, paramMap, headerMap)
	if err != nil {
		log.Printf("report rta error: %v", err)
	}
}

func (s *RtaService) sendRequest(url string, paramMap map[string]interface{}, headers map[string]string) (*http.Response, error) {
	jsonData, err := json.Marshal(paramMap)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}

	for key, value := range headers {
		req.Header.Set(key, value)
	}
	return httpClient.Do(req)
}

func generateUUID() string {
	return uuid.NewV4().String()
}
