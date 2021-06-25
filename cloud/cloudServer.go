package main

import (
	"bufio"
	"fmt"
	"k8s.io/apimachinery/pkg/util/wait"
	"log"
	"net"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"github.com/zhanshuCloud/common"
	"time"
)

var isMaster = false
var hostIp = ""
var hostMasterIp = ""

// 信息处理函数
func process(conn net.Conn) {
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
			var masterIp, signal string
			var location []string
			for j:=1; j<len(recvArray); j++{
				if recvArray[j] == "master"{
					masterIp = recvArray[j+1]
				}else if recvArray[j] == "Location"{
					location = strings.Split(recvArray[j+1], ",")
				}else if recvArray[j] == "Signal"{
					signal = recvArray[j+1]
				}
			}
			isExisted := false
			clusters := common.GetClusterInfo()
			for _, cluster := range clusters["Clusters"] {
				if cluster.Master == masterIp{
					conn.Write([]byte("The Cluster: " + masterIp + "has existed.")) // 发送数据
					isExisted=true
					break
				}
			}

			if !isExisted{
				newCluster := common.Cluster{Master: masterIp, Nodes: []string{}, Location: location, Signal: signal}
				clusters["Clusters"] = append(clusters["Clusters"], newCluster)
			} else{
				continue
			}

			setErr := common.SetClusterInfo(clusters)
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
							cmdStr := common.SendInfo(ip, recvStr)
							if cmdStr != "Add Cluster Success" {
								log.Printf("send command:%v to %v error:%v", recvStr, ip, cmdStr)
							}
						}
					}
				}
			}
		}else if recvArray[0] == "DeleteCluster"{
			//delete local ip info
			clusters := common.GetClusterInfo()
			for i, cluster := range(clusters["Clusters"]) {
				if cluster.Master == recvArray[1]{
					clusters["Clusters"] = append(clusters["Clusters"][:i], clusters["Clusters"][i+1:]...)
					break
				}
			}
			setErr := common.SetClusterInfo(clusters)
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
							cmdStr := common.SendInfo(ip, recvStr)
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
							//first, send delete current cluster to node

							//second, send join cluster cmd to node
							cmdStr := common.SendInfo(ip, recvStr)
							if cmdStr != "Start Joining Cluster" {
								log.Printf("send command:%v to %v error:%v", recvStr, ip, cmdStr)
							}
						}
					}
				}
				fmt.Println("Executing Cmd: keadm reset --force")
				cmd = exec.Command("sh", "-c", `keadm reset --force`)
				cmdOutput, cmdErr := cmd.Output()
				fmt.Println(string(cmdOutput))
				if cmdErr != nil {
					log.Println(cmdErr)
				}
			}
			requestStr := common.SendInfo(recvArray[1], "JoinRequest Cloud")
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
				fmt.Println(string(cmdOutput))
				if cmdErr != nil {
					log.Println(cmdErr)
					continue
				}
				fmt.Println("Executing Cmd: " + joinCmd)
				cmd = exec.Command("sh", "-c", joinCmd)
				cmdOutput, cmdErr = cmd.Output()
				if  cmdErr != nil {
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
			clusters := common.GetClusterInfo()
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
			setErr := common.SetClusterInfo(clusters)
			if setErr != nil{
				log.Println(setErr)
			}
		}
	}
}


func sendApplication() bool {
	fmt.Printf("Node %v apply to become a new master, need to confirm [y/N]:\n", hostIp)
	var s string
	if _, err := fmt.Scan(&s);err != nil {
		log.Println(err)
	}
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "y" {
		return true
	}else{
		return false
	}
}

func sendDeleteAndJoinInfo(info map[string][]common.Cluster) error {
	var str string
	for _, cluster := range info["Clusters"] {
		if cluster.Master == hostMasterIp { // current cluster
			for _, node := range cluster.Nodes {
				if node == hostIp {
					continue
				}
				/*
				str = "DeleteCluster " + hostMasterIp
				resp := common.SendInfo(node, str)
				log.Println(resp)

				 */

				// send join request
				str = "JoinCluster " + hostIp
				resp := common.SendInfo(node, str)
				log.Println(resp)
			}
		}else{ // other cluster masters
			/*
			str = "DeleteCluster " + hostMasterIp
			resp := common.SendInfo(cluster.Master, str)
			log.Println(resp)

			 */
		}
	}
	return nil
}

func sendAddInfo(info map[string][]common.Cluster, oldMaster string) error {
	var loc []string
	var sig string
	for _, cluster := range info["Clusters"] {
		if cluster.Master == oldMaster {
			loc = cluster.Location
			sig = cluster.Signal
			break
		}
	}

	locStr := strings.Join(loc, ",")
	str := "AddCluster master " + hostIp + "location " + locStr + " signal " + sig
	for _, cluster := range info["Clusters"] {
		resp := common.SendInfo(cluster.Master, str)
		log.Println(resp)
	}
	return nil
}


func masterStatusCheck() {
	// try to ping cluster nodes
	if hostMasterIp == "" {
		log.Printf("failed to get masterIP, waiting to join the cluster")
		return
	}
	nodeIp := common.GetNodeIp(hostMasterIp)
	if nodeIp == nil {
		log.Printf("failed to get nodeIP")
		return
	}
	isNodeConnect := common.GetNodeConnection(nodeIp, hostIp)
	if isNodeConnect == false {
		// error notification
		log.Printf("Master %s is suspected to be faulty, please verify manually\n", hostIp)
		// let edgenode wait
		// no need to update cluster information
	}
}

func workerStatusCheck() {
	// try to ping master and cluster nodes
	// ping master
	if hostMasterIp == "" {
		log.Printf("failed to get masterIP, waiting to join the cluster")
		return
	}
	isMasterConnect := common.IsConnect(hostMasterIp)

	// ping cluster nodes
	nodeIp := common.GetNodeIp(hostMasterIp)
	if nodeIp == nil {
		log.Printf("failed to get nodeIP")
		return
	}
	isNodeConnect := common.GetNodeConnection(nodeIp, hostIp)

	if isMasterConnect == false && isNodeConnect == false { // cloudnode failed
		log.Printf("Node %s is suspected to be faulty, please verify manually\n", hostIp)
		// no need to update cluster info
	}else if isMasterConnect == false && isNodeConnect == true { // master failed
		log.Printf("Suspected master %s failure, request manual verification, node %s apply to become a new master\n", hostMasterIp, hostIp)
		// send application
		reply := sendApplication() // 待输入还是运行
		if reply == true {
			// kubeadm reset
			fmt.Println("Executing Cmd: kubeadm reset -f")
			cmd := exec.Command("sh", "-c", `kubeadm reset -f`)
			cmdOutput, cmdErr := cmd.Output()
			if cmdErr !=nil {
				log.Println(cmdErr)
				return
			}
			fmt.Println(string(cmdOutput))
			// kubeadm init
			fmt.Println("Executing Cmd: kubeadm init")
			cmd = exec.Command("sh", "-c", `sh /etc/cluster/init_master.sh`)
			cmdOutput, cmdErr = cmd.Output()
			if  cmdErr !=nil {
				log.Println(cmdErr)
				return
			}
			fmt.Println(string(cmdOutput))

			// update cluster information
			// send delete cluster info
			clusterInfo := common.GetClusterInfo()
			if err := sendDeleteAndJoinInfo(clusterInfo); err != nil {
				log.Printf("send DeleteAndJoinCluster info failed: %v\n", err)
			}
			oldMasterIp := hostMasterIp
			hostMasterIp = hostIp
			// send add cluster info
			clusterInfo = common.GetClusterInfo()
			if err := sendAddInfo(clusterInfo, oldMasterIp); err != nil {
				log.Printf("send AddCluster info failed: %v\n", err)
			}
			return
		} // if "no" then wait
	}else {
		// node is normal
		return
	}
}

func main() {
	log.SetFlags(log.Lshortfile)
	listen, err := net.Listen("tcp", "0.0.0.0:9999")
	if err != nil {
		log.Println("Listen() failed, err: ", err)
		return
	}
	if _, err = os.Stat(common.ClusterConfPath); err !=nil{
		if os.IsNotExist(err) {
			log.Println(err)
			return
		}
	}

	// check whether current node is master
	hostIp, err = common.GetHostIp()
	if err != nil{
		log.Println("get host ip err: ", err)
	}

	// 异常检测
	go wait.Until(func() {
		hostMasterIp = common.GetMasterIp(hostIp)
		isMaster = hostMasterIp == hostIp
		if isMaster == true {
			masterStatusCheck()
		}else {
			workerStatusCheck()
		}
	}, time.Minute*1, nil)

	for {
		conn, err := listen.Accept() // 监听客户端的连接请求
		if err != nil {
			log.Println("Accept() failed, err: ", err)
			continue
		}
		go process(conn) // 启动一个goroutine来处理客户端的连接请求
	}
}