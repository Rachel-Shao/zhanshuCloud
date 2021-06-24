package common

import (
	"errors"
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"strings"
)

const (
	ClusterConfPath string = "/etc/cluster/conf.yaml"
	TimeOut = 1
	CmdPing = "ping %s -w %d |grep 'packets transmitted' |awk '{print $6}'"
)

type Cluster struct{
	Master string `yaml:"Master"`
	Nodes []string `yaml:"Nodes"`
	Location []string `yaml:"Location"`
	Signal string `yaml:"Signal"`
}

func SendInfo(targetIp string, info string) string{
	conn, err := net.Dial("tcp", targetIp+":9999")
	if err != nil {
		log.Println("err:", err)
		return "Failed " + err.Error()
	}
	defer conn.Close() // 关闭TCP连接

	_, err = conn.Write([]byte(info)) // 发送数据
	if err != nil {
		return "Failed " + err.Error()
	}
	buf := make([]byte, 4096)
	cnt, err := conn.Read(buf)
	if err != nil {
		log.Println("recv failed, err:", err)
		return "Failed " + err.Error()
	}
	log.Println(string(buf[:cnt]))
	recvStr := string(buf[:cnt])
	return  recvStr
}

func GetHostIp() (string, error){
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, address := range addrs {
		// 检查ip地址判断是否回环地址
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "", errors.New("Can not find host ip address!")
}


func GetMasterIp(ip string) string {
	clusterInfo := GetClusterInfo()
	for _, c := range clusterInfo["Clusters"] {
		if c.Master == ip {
			return ip
		}
		for _, nodeIp := range c.Nodes {
			if nodeIp == ip {
				return c.Master
			}
		}
	}
	return ""
}

func GetClusterInfo() map[string] []Cluster{
	info := make(map[string] []Cluster)
	recordFile, err := ioutil.ReadFile(ClusterConfPath)
	if err != nil {
		log.Println("Read cluster info failed")
	}
	err = yaml.Unmarshal(recordFile, info)
	if err != nil {
		log.Println("Unmarshal: %v", err)
	}
	return info
}

func SetClusterInfo(clusterInfo map[string] []Cluster) error{
	data, err := yaml.Marshal(clusterInfo) // 第二个表示每行的前缀，这里不用，第三个是缩进符号，这里用tab
	if err != nil{
		log.Println(err)
	}
	err = ioutil.WriteFile(ClusterConfPath, data, os.ModePerm)
	if err != nil{
		log.Println(err)
	}
	return err
}


func IsConnect(ip string) bool {
	pingCmd := fmt.Sprintf(CmdPing, ip, TimeOut)
	cmd := exec.Command("sh", "-c", pingCmd)
	cmdOutput, cmdErr := cmd.Output()
	if cmdErr != nil {
		log.Println(cmdErr)
	}
	result := strings.TrimRight(string(cmdOutput), "\n")
	if result != "0%" {
		log.Printf("ping %s timeout\n", ip)
		return false
	}
	log.Printf("ping %s success\n", ip)
	return true
}

func GetNodeIp(masterIp string) []string {
	clusterInfo := GetClusterInfo()
	for _, c := range clusterInfo["Clusters"] {
		if c.Master == masterIp {
			return c.Nodes
		}
	}
	return nil
}


func GetNodeConnection(nodeIp []string, hostIp string) bool {
	for _, ip := range nodeIp {
		if ip == hostIp {
			continue
		}
		if IsConnect(ip) == true {
			return true
		}
	}
	return false
}
