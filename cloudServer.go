package main

import (
	"bufio"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v2"
)

const ClusterConfPath string = "/etc/cluster/conf.yaml"

type Cluster struct{
	Master string `yaml:"Master"`
	Nodes []string `yaml:"Nodes"`
	Location []string `yaml:"Location"`
	Signal string `yaml:"Signal"`
}

func joinRequest(targetIp string) string{
	conn, err := net.DialTimeout("tcp", targetIp+":9999", 15 * time.Second)
	if err != nil {
		log.Println("err:", err)
		return "Failed " + err.Error()
	}
	defer conn.Close() // 关闭TCP连接

	_, err = conn.Write([]byte("JoinRequest Cloud")) // 发送数据
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

func sendCommand(targetIp string, cmd string) string{
	conn, err := net.Dial("tcp", targetIp+":9999")
	if err != nil {
		log.Println("err : ", err)
		return "connect err: "+ err.Error()
	}
	defer conn.Close() // 关闭TCP连接

	_, err = conn.Write([]byte(cmd)) // 发送数据
	if err != nil {
		return "send err: " + err.Error()
	}
	buf := make([]byte, 4096)
	cnt, err := conn.Read(buf)
	if err != nil {
		log.Println("recv failed, err:", err)
		return "read err: "+ err.Error()
	}
	log.Println(string(buf[:cnt]))
	return string(buf[:cnt])
}

func getHostIp() (string, error){
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


func getClusterInfo() map[string] []Cluster{
	info := make(map[string] []Cluster)
	recordFile, err := ioutil.ReadFile(ClusterConfPath)
	if err != nil {
		log.Println("Read cluster info failed")
		return nil
	}
	err = yaml.Unmarshal(recordFile, info)
	if err != nil {
		log.Println("Unmarshal: %v", err)
		return nil
	}
	return info
}

func setClusterInfo(clusterInfo map[string] []Cluster) error{
	data, err := yaml.Marshal(clusterInfo)	// 第二个表示每行的前缀，这里不用，第三个是缩进符号，这里用tab
	if err != nil{
		log.Println(err)
		return err
	}
	err = ioutil.WriteFile(ClusterConfPath, data, os.ModePerm)
	if err != nil{
		log.Println(err)
		return err
	}
	return nil
}

// TCP Server端测试
// 处理函数
func process(conn net.Conn) {
	isMaster := false
	// check whether current node is master
	hostIp,err := getHostIp()
	if err != nil{
		log.Println("get host ip err: ", err)
	}else{
		clusterInfo := getClusterInfo()
		for _, cluster := range clusterInfo["Clusters"] {
			if hostIp == cluster.Master{
				isMaster = true
				break
			}
		}
	}

	defer conn.Close() // 关闭连接
	for {
		buf := make([]byte, 4096)
		reader := bufio.NewReader(conn)
		cnt, err := reader.Read(buf)
		if err != nil {
			log.Println("read from client failed, err: ", err)
			break
		}
		recvStr := strings.TrimSpace(string(buf[0:cnt]))
		log.Println("收到Client端发来的数据：", recvStr)
		recvArray := strings.Fields(strings.TrimSpace(recvStr))

		if recvArray[0] == "AddCluster"{
			//update local ip info
			isExisted := false
			clusters := getClusterInfo()
			for _, cluster := range(clusters["Clusters"]) {
				if cluster.Master == recvArray[2]{
					conn.Write([]byte("The Cluster: "+recvArray[2]+"has existed.")) // 发送数据
					isExisted=true
					break
				}
			}
			if !isExisted{
				loc:= strings.Split(recvArray[4], ",")
				new_cluster := Cluster{recvArray[2], []string{},loc, recvArray[6]}
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
			//if master, send info to all nodes
			if isMaster {
				cmd := exec.Command("sh", "-c", `kubectl get no -owide | grep -v master |awk '{print $6}'`)
				if ipaddress, err := cmd.Output(); err != nil {
					log.Println("get node ip err: ", err)
					continue
				} else {
					ipArray := strings.Split(string(ipaddress), "\n")
					for _,ip := range ipArray{
						if ip != "INTERNAL-IP"{
							//send info to node
							cmdStr := sendCommand(ip, recvStr)
							if cmdStr != "Add Cluster Success" {
								log.Printf("send command:%v to %v error:%v", recvStr, ip, cmdStr)
							}
						}
					}
				}
			}
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
			if isMaster {
				cmd := exec.Command("sh", "-c", `kubectl get no -owide | grep -v master |awk '{print $6}'`)
				if ipaddress, err := cmd.Output(); err != nil {
					log.Println("get node ip err: ", err)
					continue
				} else {
					ipArray := strings.Split(string(ipaddress), "\n")
					for _,ip := range ipArray{
						if ip != "INTERNAL-IP"{
							//send info to node
							cmdStr := sendCommand(ip, recvStr)
							if cmdStr != "Delete Cluster Success" {
								log.Printf("send command:%v to %v error:%v", recvStr, ip, cmdStr)
							}
						}
					}
				}
			}
		}else if recvArray[0] == "JoinCluster"{
			if isMaster {
				log.Println("Executing Cmd: kubectl get no -owide | grep -v master |awk '{print $6}'")
				cmd := exec.Command("sh", "-c", `kubectl get no -owide | grep -v master |awk '{print $6}'`)
				if ipaddress, err := cmd.Output(); err != nil {
					log.Println("get node ip err: ", err)
					continue
				} else {
					ipArray := strings.Split(string(ipaddress), "\n")
					for _,ip := range ipArray {
						if ip != "INTERNAL-IP" {
							//send info to node
							cmdStr := sendCommand(ip, recvStr)
							if cmdStr != "Start Joining Cluster" {
								log.Printf("send command:%v to %v error:%v", recvStr, ip, cmdStr)
							}
						}
					}
				}
			}
			requestStr := joinRequest(recvArray[1])
			strArray := strings.Fields(requestStr)
			if strArray[0] == "Failed" {
				conn.Write([]byte("Request Joining Cluster:" + recvArray[1] + " Error:" + requestStr)) // 发送数据
				log.Printf("Request Joioning Cluster:%v Error:%v", recvArray[1], requestStr)

			}else {
				re, _ :=regexp.Compile(".*:6443")
				joinCmd := re.ReplaceAllString(requestStr, "kubeadm join " + recvArray[1] + ":6443")
				conn.Write([]byte("Start Joining Cluster")) // 发送数据
				fmt.Println("Executing Cmd: kubeadm reset -f")
				cmd := exec.Command("sh", "-c", `kubeadm reset -f`)
				cmdOutput, cmdErr := cmd.Output()
				if cmdErr !=nil {
					log.Println(cmdErr)
					continue
				}
				fmt.Println(string(cmdOutput))
				fmt.Println("Executing Cmd: " + joinCmd)
				cmd = exec.Command("sh", "-c", joinCmd)
				cmdOutput, cmdErr = cmd.Output()
				if  cmdErr !=nil {
					log.Println(cmdErr)
					continue
				}
				fmt.Println(string(cmdOutput))
			}
		}else if recvArray[0] == "JoinRequest"{
			if recvArray[1] == "Cloud" { // Cloud Node Join Request
				cmdJoin := exec.Command("sh", "-c", `kubeadm token create --print-join-command`)
				if cmdOutput, err := cmdJoin.Output(); err != nil {
					log.Println("get node join cmd err: ", err)
					conn.Write([]byte("Failed " + err.Error()))
				} else {
					log.Println(string(cmdOutput))
					conn.Write(cmdOutput) // 发送数据
				}
			}else { // Edge Node Join Request
				cmdToken := exec.Command("sh", "-c", `keadm gettoken`)
				if cmdOutput, err := cmdToken.Output(); err != nil {
					log.Println("get ke token err: ", err)
					conn.Write([]byte("Failed " + err.Error()))
				} else {
					log.Println(string(cmdOutput))
					conn.Write(cmdOutput) // 发送数据
				}
			}
		}else if recvArray[0] == "UpdateCluster"{ //更新集群信息
			clusters := getClusterInfo()
			for i, cluster := range(clusters["Clusters"]) {
				if cluster.Master == recvArray[2]{
					for j:=3; j<len(recvArray); j++{
						if recvArray[j] == "Nodes"{
							nodes := strings.Split(recvArray[j+1], ",")
							clusters["Clusters"][i].Nodes = nodes
						}else if recvArray[j] == "Location"{
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
		return
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