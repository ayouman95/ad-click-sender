package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
)

func getWorkerIDByIP() int64 {
	// 获取所有网络接口
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		panic(fmt.Sprintf("无法获取IP地址: %v", err))
	}

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				// IPv4
				return int64(ipnet.IP.To4()[3]) // 最后一个字节
			}
		}
	}

	// fallback
	fmt.Println("未找到有效IP，使用主机名hash")
	return int64(getWorkerIDFromHostname())
}

func getWorkerIDFromHostname() int64 {
	hostname, err := os.Hostname()
	if err != nil {
		return 1
	}

	var hash uint32
	for _, c := range hostname {
		hash = hash*31 + uint32(c)
	}

	return int64(hash % 1024) // 支持最多 1024 个节点
}

func getWorkerIDByMAC() int64 {
	interfaces, err := net.Interfaces()
	if err != nil {
		return 1
	}

	for _, iface := range interfaces {
		if iface.HardwareAddr != nil && !isLoopback(&iface) {
			mac := iface.HardwareAddr
			// 用 MAC 前6字节计算 hash
			var hash uint32
			for i := 0; i < 6; i++ {
				hash = hash*31 + uint32(mac[i])
			}
			return int64(hash % 1024)
		}
	}
	return 1
}

func isLoopback(iface *net.Interface) bool {
	return (iface.Flags & net.FlagLoopback) != 0
}

func getWorkerID() int64 {
	// 优先使用环境变量（最高优先级）
	if idStr := os.Getenv("SNOWFLAKE_WORKER_ID"); idStr != "" {
		if id, err := strconv.ParseInt(idStr, 10, 64); err == nil {
			return id
		}
	}

	// 其次尝试 IP
	if id := getWorkerIDByIP(); id != 1 {
		return id
	}

	// 再次尝试 MAC
	if id := getWorkerIDByMAC(); id != 1 {
		return id
	}

	// 最后 fallback 到 hostname
	return getWorkerIDFromHostname()
}
