package main

import (
	"bufio"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strings"
)

const ClusterConfPath string = "/etc/cluster/conf.yaml"

type Cluster struct{
	Master string `yaml:"Master"`
	Location []string `yaml:"Location"`
	Signal string `yaml:"Signal"`
}

func joinRequest(targetIp string) string{
	conn, err := net.Dial("tcp", targetIp+":9999")
	if err != nil {
		log.Println("err:", err)
		return err.Error()
	}
	defer conn.Close() // 关闭TCP连接

	_, err = conn.Write([]byte("JoinRequest Edge")) // 发送数据
	if err != nil {
		return err.Error()
	}
	buf := [512]byte{}
	n, err := conn.Read(buf[:])
	if err != nil {
		log.Println("recv failed, err:", err)
		return err.Error()
	}
	log.Println(string(buf[:n]))
	recvStr := string(buf[:n])
	return  recvStr
}

func getClusterInfo() map[string] []Cluster{
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

func setClusterInfo(clusterInfo map[string] []Cluster) error{
	data, err := yaml.Marshal(clusterInfo)	// 第二个表示每行的前缀，这里不用，第三个是缩进符号，这里用tab
	if err != nil{
		log.Println(err)
	}
	err = ioutil.WriteFile(ClusterConfPath, data, os.ModePerm)
	if err != nil{
		log.Println(err)
	}
	return err
}

// TCP Server端测试
// 处理函数
func process(conn net.Conn) {
	defer conn.Close() // 关闭连接
	for {
		reader := bufio.NewReader(conn)
		var buf [128]byte
		n, err := reader.Read(buf[:]) // 读取数据
		if err != nil {
			log.Println("read from client failed, err:", err)
			break
		}
		recvStr := string(buf[:n])
		log.Println("收到Client端发来的数据：", recvStr)
		recvArray := strings.Fields(strings.TrimSpace(recvStr))

		if recvArray[0] == "AddCluster"{
			//update local ip info
			isExisted := false
			clusters := getClusterInfo()
			for _, cluster := range(clusters["Clusters"]) {
				if cluster.Master == recvArray[2]{
					conn.Write([]byte("The Cluster:"+recvArray[2]+" has existed.")) // 发送数据
					isExisted=true
					break
				}
			}
			if !isExisted{
				loc:= strings.Split(recvArray[4], ",")
				new_cluster := Cluster{recvArray[2], loc, recvArray[6]}
				clusters["Clusters"] = append(clusters["Clusters"], new_cluster)
			} else{
				continue
			}

			setErr := setClusterInfo(clusters)
			if setErr != nil {
				log.Println(setErr)
				continue
			}
			conn.Write([]byte("Add Cluster Success")) // 发送数据

		}else if recvArray[0] == "DeleteCluster"{
			//delete local ip info
			clusters := getClusterInfo()
			for i, cluster := range(clusters["Clusters"]) {
				if cluster.Master == recvArray[1]{
					clusters["Clusters"] = append(clusters["Clusters"][:i], clusters["Clusters"][i+1:]...)
					break
				}
			}
			setErr := setClusterInfo(clusters)
			if setErr != nil{
				log.Println(setErr)
				continue
			}
			conn.Write([]byte("Delete Cluster Success")) // 发送数据
		}else if recvArray[0] == "JoinCluster"{
			requestStr := joinRequest(recvArray[1])
			strArray := strings.Fields(requestStr)
			if strArray[0] == "Failed"{
				conn.Write([]byte("Request Joining Cluster:" + recvArray[1] + " Error:" + requestStr)) // 发送数据
				log.Printf("Request Joioning Cluster:%v Error:%v", recvArray[1], requestStr)
			}else {
				log.Println(strArray[1])
				conn.Write([]byte("Start Joining Cluster")) // 发送数据
			}
		}else if recvArray[0] == "UpdateCluster"{ //更新集群信息
			clusters := getClusterInfo()
			for i, cluster := range(clusters["Clusters"]) {
				if cluster.Master == recvArray[2]{
					for j:=3; j<len(recvArray); j++{
						if recvArray[j] == "Location"{
							loc := strings.Split(recvArray[j+1], ",")
							clusters["Clusters"][i].Location = loc
						}else if recvArray[j] == "Signal"{
							clusters["Clusters"][i].Signal = recvArray[j+1]
						}
					}
				}
			}
			setErr := setClusterInfo(clusters)
			if setErr != nil{
				log.Println(setErr)
			}
		}
	}
}

func main() {
	log.SetFlags(log.Lshortfile)
	listen, err := net.Listen("tcp", "0.0.0.0:9999")
	if err != nil {
		log.Println("Listen() failed, err: ", err)
	}
	if _, err := os.Stat(ClusterConfPath); err !=nil{
		if os.IsNotExist(err) {
			log.Println(err)
			return
		}
	}
	for {
		conn, err := listen.Accept() // 监听客户端的连接请求
		if err != nil {
			log.Println("Accept() failed, err: ", err)
			continue
		}
		go process(conn) // 启动一个goroutine来处理客户端的连接请求
	}
}
