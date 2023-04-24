package util

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
)

// 消息类型
const (
	Proposal = iota
	Promise
	Accept
	Ack
	IP           = "localhost"
	ProposerPort = 9090
)

type Msg struct {
	N          int  `json:"n"`           // 提案编号
	V          int  `json:"v"`           // 提案值
	MsgType    int  `json:"msg_type"`    // 消息类型
	IsNil      bool `json:"is_nil"`      // 是否为nil
	IsRejected bool `json:"is_rejected"` // 是否被拒绝
}

type Proposer struct {
	Addr           string `json:"addr"`      // 提案者的ip地址
	Proposal       Msg    `json:"proposal"`  // 提案
	MaxAcceptValue int    `json:"max_value"` // promise中最大的提案编号对应的提案值
}

type Acceptor struct {
	Addr              string `json:"addr"`                // 接收者的ip地址
	MaxNumberReceived int    `json:"max_number_received"` // 收到的最大的提案编号
	MaxNumberAccepted int    `json:"max_number_accepted"` // 接受的最大的提案编号
	Value             int    `json:"value"`               // 接受的最大的提案编号所对应的提案值
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func GenerateTLSConfig() *tls.Config {
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		panic(err)
	}
	template := x509.Certificate{SerialNumber: big.NewInt(1)}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		panic(err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		NextProtos:   []string{"quic-server-example"},
	}
}
