package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

// TCP 客户端
func main() {
	for {
		inputReader := bufio.NewReader(os.Stdin)
		fmt.Println("Please input target server")
		inputString, _ := inputReader.ReadString('\n') // 读取用户输入
		targetServer := strings.Trim(inputString, "\r\n")
		conn, err := net.Dial("tcp", targetServer)
		if err != nil {
			fmt.Println("err : ", err)
			return
		}
		defer conn.Close() // 关闭TCP连接

		for {
			input, _ := inputReader.ReadString('\n') // 读取用户输入
			inputInfo := strings.Trim(input, "\r\n")
			if strings.ToUpper(inputInfo) == "EXIT" { // 如果输入Exit就退出
				return
			}
			if strings.ToUpper(inputInfo) == "Q" { // 如果输入q就退出当前会话
				break
			}
			_, err := conn.Write([]byte(inputInfo)) // 发送数据
			if err != nil {
				break
			}
			buf := make([]byte, 4096)
			cnt, err := conn.Read(buf)
			if err != nil {
				fmt.Println("recv failed, err:", err)
				break
			}
			fmt.Println(string(buf[:cnt]))
		}
	}

}
