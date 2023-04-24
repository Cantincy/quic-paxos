package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/quic-go/quic-go"
	"log"
	rd "math/rand"
	"pro01/quic/paxos/util"
)

var acceptorPorts []int

func genAcceptorPort(acceptorNum int) {
	for i := 1; i <= acceptorNum; i++ {
		acceptorPorts = append(acceptorPorts, util.ProposerPort+i)
	}
}

func main() {
	var acceptorNum int // 接收者的数量
	var beginNumber int // 第一个提案的编号
	flag.IntVar(&acceptorNum, "acceptorNum", 5, "The number of the acceptors")
	flag.IntVar(&beginNumber, "beginNumber", 0, "The number of the first proposal")
	flag.Parse()

	// 为所有acceptor分配Port
	genAcceptorPort(acceptorNum)

	// 开启acceptor服务（协程+Quic）
	for i := 0; i < acceptorNum; i++ {
		go func(port, i int) {
			addr := fmt.Sprintf("%s:%d", util.IP, port)                                          // 构造addr
			acceptor := &util.Acceptor{Addr: addr, MaxNumberAccepted: -1, MaxNumberReceived: -1} // 初始化acceptor对象

			// 监听请求
			listener, err := quic.ListenAddr(addr, util.GenerateTLSConfig(), nil)
			defer listener.Close()
			if err != nil {
				log.Fatal(err)
			}

			// 建立 proposer->acceptor 单向连接
			connFromProposer, err := listener.Accept(context.Background())
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("[Acceptor%d]:建立来自Proposer的连接\n", i)

			/*
				1.2 接收Proposal
			*/
			// 读取stream流
			stream0, err := connFromProposer.AcceptStream(context.Background())
			if err != nil {
				log.Fatal(err)
			}
			// 从stream流中读取Proposal
			buf := make([]byte, 100)
			stream0.Read(buf)
			stream0.Close()

			log.Printf("[Acceptor%d]:从stream读取proposal\n", i)

			var proposal util.Msg
			err = jsoniter.Unmarshal(buf, &proposal)
			if err != nil {
				log.Fatal(err)
			}
			if proposal.MsgType != util.Proposal {
				log.Fatal(fmt.Errorf("not proposal"))
			}

			// 提案编号 > 收到过的最大编号
			promise := util.Msg{MsgType: util.Promise}
			if proposal.N > acceptor.MaxNumberReceived {
				if acceptor.MaxNumberAccepted != -1 { //接受过的最大编号的提案
					promise.N = acceptor.MaxNumberAccepted
					promise.V = acceptor.Value
					acceptor.MaxNumberReceived = util.Max(acceptor.MaxNumberReceived, proposal.N) //更新收到过的最大编号
				} else { //没接受过任何提案
					promise.IsNil = true
				}
			} else { //提案编号 <= 收到过的最大编号，返回Reject
				promise.IsRejected = true
			}
			//log.Println(promise.IsNil, promise.IsRejected)

			/*
				1.2 返回promise响应
			*/
			proposerAddr := fmt.Sprintf("%s:%d", util.IP, util.ProposerPort)
			tlsConf := &tls.Config{
				InsecureSkipVerify: true,
				NextProtos:         []string{"quic-server-example"},
			}

			// 建立 acceptor->proposer 单向连接
			connToProposer, err := quic.DialAddr(proposerAddr, tlsConf, nil)
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("[Acceptor%d]:向Proposer建立连接\n", i)

			// 在conn中初始化一个stream
			stream1, err := connToProposer.OpenStreamSync(context.Background())
			if err != nil {
				log.Fatal(err)
			}

			// 向stream中写入promise
			jsonData, err := jsoniter.Marshal(promise)
			if err != nil {
				log.Fatal(err)
			}
			_, err = stream1.Write(jsonData)
			if err != nil {
				log.Fatal(err)
			}
			stream1.Close()

			log.Printf("[Acceptor%d]:向stream写入promise\n", i)

			// 2.1 acceptor接收accept请求
			stream2, err := connFromProposer.AcceptStream(context.Background())
			if err != nil {
				log.Fatal(err)
			}

			buf = make([]byte, 100)
			stream2.Read(buf)
			stream2.Close()

			log.Printf("[Acceptor%d]:从stream读取accept\n", i)

			var accept util.Msg
			err = jsoniter.Unmarshal(buf, &accept)
			if err != nil {
				log.Fatal(err)
			}

			// 如果不是Accept请求
			if accept.MsgType != util.Accept {
				log.Fatal(fmt.Errorf("not accept"))
			}

			//	2.1 返回ack响应（即采纳该提案）
			ack := util.Msg{MsgType: util.Ack}
			if accept.N < acceptor.MaxNumberReceived { // 提案编号 < 接收者收到过的最大编号
				ack.IsRejected = true
			}
			stream3, err := connToProposer.OpenStreamSync(context.Background())
			if err != nil {
				log.Fatal(err)
			}

			jsonData, err = jsoniter.Marshal(ack)
			if err != nil {
				log.Fatal(err)
			}

			//向stream写入ack
			_, err = stream3.Write(jsonData)
			if err != nil {
				log.Fatal(err)
			}
			stream3.Close()
			log.Printf("[Acceptor%d]:向stream写入ack\n", i)

		}(acceptorPorts[i], i)
	}

	//==================================================================================================================

	// proposer协程（主协程）
	addr := fmt.Sprintf("%s:%d", util.IP, util.ProposerPort)
	Proposer := &util.Proposer{Addr: addr, Proposal: util.Msg{N: beginNumber, V: 0, MsgType: util.Proposal}, MaxAcceptValue: -1}

	// acceptor->proposer 单向连接
	connsFromAcceptor := make([]quic.Connection, 0, acceptorNum)

	// proposer->acceptor 单向连接
	connsToAcceptor := make([]quic.Connection, 0, acceptorNum)

	/*
		1.1 向所有acceptor发送proposal
	*/
	for i := 0; i < acceptorNum; i++ {
		tlsConf := &tls.Config{
			InsecureSkipVerify: true,
			NextProtos:         []string{"quic-server-example"},
		}
		// 向acceptor发起连接
		acceptorAddr := fmt.Sprintf("%s:%d", util.IP, acceptorPorts[i])

		// proposer->acceptor 单向连接
		conn, err := quic.DialAddr(acceptorAddr, tlsConf, nil)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("[Proposer]:向Acceptor%d建立连接\n", i)
		connsToAcceptor = append(connsToAcceptor, conn)

		// 在conn中初始化一个stream
		stream0, err := conn.OpenStreamSync(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		// 向stream中写入提案数据
		jsonData, err := jsoniter.Marshal(Proposer.Proposal)
		if err != nil {
			log.Fatal(err)
		}
		_, err = stream0.Write(jsonData)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("[Proposer]:向Acceptor%d-Conn的stream写入proposal\n", i)

		stream0.Close()
	}

	// 监听请求
	listener, err := quic.ListenAddr(addr, util.GenerateTLSConfig(), nil)
	if err != nil {
		log.Fatal(err)
	}

	/*
		1.2 接收promise响应
	*/

	maxNumber := -1          //promise中最大的编号number
	maxValue := rd.Intn(101) //promise中最大编号对应的value

	validPromiseCnt := 0 // 有效的promise响应的个数
	for i := 0; i < acceptorNum; i++ {
		// 建立 acceptor->proposer 单向连接
		conn, err := listener.Accept(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		connsFromAcceptor = append(connsFromAcceptor, conn)
		log.Printf("[Proposer]:建立来自Acceptor%d的连接\n", i)
		// 读取stream流
		stream1, err := conn.AcceptStream(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		// 从stream流中读取Promise
		buf := make([]byte, 100)
		stream1.Read(buf)
		stream1.Close()

		log.Printf("[Proposer]:从Acceptor%d-Conn的stream读取promise\n", i)

		var msg util.Msg
		err = jsoniter.Unmarshal(buf, &msg)
		if err != nil {
			log.Fatal(err)
		}
		if msg.MsgType != util.Promise {
			log.Fatal(fmt.Errorf("not promise"))
		}
		if msg.IsRejected == false {
			validPromiseCnt++
			if msg.IsNil == false {
				if maxNumber < msg.N {
					maxNumber = msg.N
					maxValue = msg.V
				}
			}
		}
	}

	log.Println("validPromiseCnt:", validPromiseCnt)

	// 超过半数的有效Promise响应
	if validPromiseCnt > acceptorNum/2 {
		Proposer.MaxAcceptValue = maxValue
	} else {
		log.Fatal("not enough promise ack")
	}

	/*
		2.1 proposer发送accept请求
	*/
	for i := 0; i < acceptorNum; i++ {
		// 在conn中初始化一个stream
		stream2, err := connsToAcceptor[i].OpenStreamSync(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		accept := util.Msg{N: Proposer.Proposal.N, V: Proposer.MaxAcceptValue, MsgType: util.Accept}

		// 向stream中写入提案数据
		jsonData, err := jsoniter.Marshal(accept)
		if err != nil {
			log.Fatal(err)
		}
		_, err = stream2.Write(jsonData)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("[Proposer]:向Acceptor%d-Conn的stream写入accept\n", i)

		stream2.Close()
	}

	/*
		2.2 proposer接收ack响应
	*/
	validAckCnt := 0
	for i := 0; i < acceptorNum; i++ {
		// 在conn中初始化一个stream
		stream3, err := connsFromAcceptor[i].AcceptStream(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		// 从stream读取ack
		buf := make([]byte, 100)
		stream3.Read(buf)
		stream3.Close()

		log.Printf("[Proposer]:从Acceptor%d-Conn的stream读取ack\n", i)

		var ack util.Msg
		err = jsoniter.Unmarshal(buf, &ack)
		if err != nil {
			log.Fatal(err)
		}

		if ack.IsRejected == false {
			validAckCnt++
		}

	}
	log.Println("validAckCnt:", validAckCnt)

	if validAckCnt > acceptorNum/2 {
		log.Println("达成共识:number=", Proposer.Proposal.N)
	} else {
		log.Println("未达成共识")
	}
}
