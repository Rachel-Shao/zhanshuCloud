package main


import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"

	"../common"
)

var (
	CurrentMasterIP string
	CurrentHostIP string
)

// TCP Server端测试
// 处理函数
func process(conn net.Conn) {
	defer conn.Close() // 关闭连接
	for {
		buf := make([]byte, 4096)
		reader := bufio.NewReader(conn)
		cnt, err := reader.Read(buf)
		if err != nil {
			log.Println("read from client failed, err:", err)
			break
		}
		recvStr := string(buf[:cnt])
		log.Println("收到Client端发来的数据：", recvStr)
		recvArray := strings.Fields(strings.TrimSpace(recvStr))

		if recvArray[0] == "AddCluster"{
			//update local ip info
			isExisted := false
			clusters := common.GetClusterInfo()
			for _, cluster := range(clusters["Clusters"]) {
				if cluster.Master == recvArray[2]{
					conn.Write([]byte("The Cluster:"+recvArray[2]+" has existed.")) // 发送数据
					isExisted=true
					break
				}
			}
			if !isExisted{
				loc:= strings.Split(recvArray[4], ",")
				new_cluster := common.Cluster{recvArray[2], []string{},loc, recvArray[6]}
				clusters["Clusters"] = append(clusters["Clusters"], new_cluster)
			} else{
				continue
			}

			setErr := common.SetClusterInfo(clusters)
			if setErr != nil {
				log.Println(setErr)
				continue
			}
			conn.Write([]byte("Add Cluster Success")) // 发送数据

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
		}else if recvArray[0] == "JoinCluster"{
			requestStr := common.SendInfo(recvArray[1], "JoinRequest Edge")
			strArray := strings.Fields(requestStr)
			if strArray[0] == "Failed"{
				conn.Write([]byte("Request Joining Cluster:" + recvArray[1] + " Error:" + requestStr)) // 发送数据
				log.Printf("Request Joioning Cluster:%v Error:%v", recvArray[1], requestStr)
			}else {
				log.Println(strArray[1])
				conn.Write([]byte("Start Joining Cluster")) // 发送数据
				fmt.Println("Executing Cmd: kubeadm reset -f")
				cmd := exec.Command("sh", "-c", `keadm reset --force`)
				cmdOutput, cmdErr := cmd.Output()
				fmt.Println(string(cmdOutput))
				if cmdErr != nil {
					log.Println(cmdErr)
				}

				fmt.Printf("Executing Cmd: keadm join --tarballpath=/etc/kubeedge --kubeedge-version=1.5.0 --cloudcore-ipport=%s:10000 -t %s",recvArray[1], requestStr)
				joinCmd := fmt.Sprintf("keadm join --tarballpath=/etc/kubeedge --kubeedge-version=1.5.0 --cloudcore-ipport=%s:10000 -t %s",recvArray[1], requestStr)
				cmd = exec.Command("sh", "-c", joinCmd)
				cmdOutput, cmdErr = cmd.Output()
				if  cmdErr != nil {
					log.Println(cmdErr)
					continue
				}
				fmt.Println(string(cmdOutput))
			}
		}else if recvArray[0] == "UpdateCluster"{ //更新集群信息
			clusters := common.GetClusterInfo()
			for i, cluster := range(clusters["Clusters"]) {
				if cluster.Master == recvArray[2]{
					for j:=3; j<len(recvArray); j++{
						if recvArray[j] == "Nodes" {
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


func getBestCluster(info map[string][]common.Cluster) *common.Cluster {
	maxSignal := 0
	bestCluster := common.Cluster{}
	for _, cluster := range info["Clusters"] {
		sg, _ := strconv.Atoi(cluster.Signal)
		if CurrentMasterIP != cluster.Master && common.IsConnect(cluster.Master) && sg > maxSignal {
			maxSignal = sg
			bestCluster = cluster
		}
	}
	if bestCluster.Master == "" {
		return nil
	}
	return &bestCluster
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

func reJoin(cluster *common.Cluster) error {
	// 发送join消息，接收token
	requestStr := joinRequest(cluster.Master) // only token
	strArray := strings.Fields(requestStr)
	if strArray[0] == "Failed" {
		return errors.New("failed to get ke join token")
	}else {
		cmd := exec.Command("sh", "keadm join ",
			"--cloudcore-ipport", cluster.Master,
			"--token", requestStr)
		if cmdOutput, err := cmd.Output(); err != nil {
			return err
		}else {
			fmt.Println(string(cmdOutput))
			return nil
		}
	}
}


func sendUpdateInfo(info map[string][]common.Cluster, newCluster common.Cluster) error {
	for _, cluster := range info["Clusters"] {
		if cluster.Master == CurrentMasterIP {
			// update master info
			nodeStr := strings.Join(newCluster.Nodes, ",")
			str := "UpdateCluster master " + newCluster.Master + " nodes " + nodeStr
			resp := common.SendInfo(cluster.Master, str)
			log.Println(resp)

			for _, node := range cluster.Nodes {
				// update node info
				nodeStr := strings.Join(newCluster.Nodes, ",")
				str := "UpdateCluster master " + newCluster.Master + " nodes " + nodeStr
				resp := common.SendInfo(node, str)
				log.Println(resp)
			}
		}
	}
	return nil
}

func statusCheck() {
	if CurrentMasterIP == "" {
		log.Printf("failed to get masterIP, waiting to join the cluster")
		return
	}
	// Check the connection status between cloud and edge
	// ping master
	isMasterConnect := common.IsConnect(CurrentMasterIP)

	// ping node
	nodeIp := common.GetNodeIp(CurrentMasterIP)
	if nodeIp == nil {
		log.Printf("failed to get nodeIP")
		return
	}
	isNodeConnect := common.GetNodeConnection(nodeIp, CurrentHostIP)

	// If there is no stable connection, edgeNode will automatically switch clusters
	if isMasterConnect == false && isNodeConnect == false {
		// delete cluster info
		fmt.Println("Executing Cmd: keadm reset -f")
		cmd := exec.Command("sh", "-c", `keadm reset -f`)
		cmdOutput, cmdErr := cmd.Output()
		if cmdErr !=nil {
			log.Println(cmdErr)
			return
		}
		fmt.Println(string(cmdOutput))
		// send update cluster info
		clusterInfo := common.GetClusterInfo()
		var newCluster common.Cluster
		for _, cluster := range clusterInfo["Clusters"] {
			if cluster.Master == CurrentMasterIP {
				var newNodes []string
				for _, node := range cluster.Nodes {
					if node != CurrentHostIP {
						newNodes = append(newNodes, node)
					}
				}
				cluster.Nodes = newNodes
				newCluster = cluster
			}
		}
		if err := sendUpdateInfo(clusterInfo, newCluster); err != nil {
			log.Printf("send UpdateCluster info failed: %v\n", err)
		}


		// choose cluster to join
		bestCluster := getBestCluster(clusterInfo)
		if bestCluster == nil {
			log.Println("No cluster available, keep state")
			return
		}
		err := reJoin(bestCluster)
		var maxRetryTimes = 3
		for i := 1; err != nil; i++ {
			log.Printf("failed to rejoin the new cluster: %v\n", err)
			if i == maxRetryTimes {
				log.Println("failed to rejoin the new cluster after #{i} times try")
				break
			}
			err = reJoin(bestCluster)
		}
		CurrentMasterIP = bestCluster.Master
		// wait to update info
	}
}

func main() {
	log.SetFlags(log.Lshortfile)
	listen, err := net.Listen("tcp", "0.0.0.0:9999")
	if err != nil {
		log.Println("Listen() failed, err: ", err)
	}
	if _, err := os.Stat(common.ClusterConfPath); err !=nil{
		if os.IsNotExist(err) {
			log.Println(err)
			return
		}
	}

	// init
	CurrentHostIP, err = common.GetHostIp()
	if err != nil {
		log.Println("get host ip err: ", err)
		return
	}
	CurrentMasterIP = common.GetMasterIp(CurrentHostIP)
	// 异常检测
	go wait.Until(func() {
		statusCheck()
	}, time.Minute*5, nil)

	for {
		conn, err := listen.Accept() // 监听客户端的连接请求
		if err != nil {
			log.Println("Accept() failed, err: ", err)
			continue
		}
		go process(conn) // 启动一个goroutine来处理客户端的连接请求
	}
}
