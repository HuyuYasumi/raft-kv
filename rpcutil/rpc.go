package rpcutil

import (
	"log"
	"net/rpc"
)

type ClientEnd struct {
	Addr   string
	Client *rpc.Client
}

func (e *ClientEnd) Call(methodName string, args interface{}, reply interface{}) bool {
	if e.Client == nil {
		e.Client = TryConnect(e.Addr)
		if e.Client == nil {
			return false
		}
	}

	err := e.Client.Call(methodName, args, reply)
	if err != nil {
		log.Println(err)
		e.Client = nil
		return false
	}
	return true
}

func TryConnect(address string) *rpc.Client {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Println(err)
		return nil
	}

	return client
}
