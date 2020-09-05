package main

import (
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"

	"github.com/HuyuYasumi/raft-kv/kv"
	"github.com/HuyuYasumi/raft-kv/raft"
	"github.com/HuyuYasumi/raft-kv/rpcutil"
)

type Config struct {
	Servers []struct {
		Ip   string
		Port string
	}
	Me int `yaml:"me"`
}

func main() {
	sercfg := getcfg()
	i := len(sercfg.Servers)
	if (i & 1) == 0 {
		panic("总服务器数量必须为单数")
	}

	clientEnds := getClientEnds(sercfg)
	persister := raft.MakePersister()
	kvur := kv.StartKVServer(clientEnds, sercfg.Me, persister, i)

	if err := rpc.Register(kvur); err != nil {
		panic(err)
	}

	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":"+sercfg.Servers[sercfg.Me].Port)
	if e != nil {
		panic(e)
	}
	log.Println("Listen", sercfg.Servers[sercfg.Me].Port)

	http.Serve(l, nil)
}

func getClientEnds(sercfg Config) []*rpcutil.ClientEnd {
	clientEnds := make([]*rpcutil.ClientEnd, 0)
	for i, end := range sercfg.Servers {
		address := end.Ip + ":" + end.Port
		var client *rpc.Client
		if i == sercfg.Me {
			client = nil
		} else {
			client = rpcutil.TryConnect(address)
		}

		ce := &rpcutil.ClientEnd{
			Addr:   address,
			Client: client,
		}

		clientEnds = append(clientEnds, ce)
	}
	return clientEnds
}

func getcfg() Config {
	var path string
	if len(os.Args) == 1 {
		path = "config/server.yml"
	} else {
		path = os.Args[1]
	}
	cfgbt, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}

	config := Config{}
	err = yaml.Unmarshal(cfgbt, &config)
	if err != nil {
		panic(err)
	}
	return config
}
