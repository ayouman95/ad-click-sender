package rta

import (
	"fmt"
	"github.com/lionsoul2014/ip2region/binding/golang/xdb"
)

// 1、从 dbPath 加载整个 xdb 到内存
const (
	dbPath = "./ip2region.xdb"
)

var searcher *xdb.Searcher

func init() {
	cBuff, err := xdb.LoadContentFromFile(dbPath)
	if err != nil {
		fmt.Printf("failed to load content from `%s`: %s\n", dbPath, err)
		return
	}

	// 2、用全局的 cBuff 创建完全基于内存的查询对象。
	searcher, err = xdb.NewWithBuffer(cBuff)
	if err != nil {
		fmt.Printf("failed to create searcher with content: %s\n", err)
		return
	}
}

func searchIp(ip string) string {
	result, err := searcher.SearchByStr(ip)
	if err != nil {
		fmt.Printf("failed to search ip [%s]: %s\n", ip, err)
		return "unknow"
	}
	return result
}
