package main

import (
	"bufio"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"kvuR/kv"
	"kvuR/rpcutil"
	"log"
	"os"
	"strings"
)

type ClientConfig struct {
	ClientEnd []struct {
		Ip   string
		Port string
	} `yaml:"servers"`
}

type Command struct {
	Name   string
	Num    int
	Action func(args []string)
}

const (
	EXIT   = "exit"
	TIP    = "> "
	GET    = "get"
	APPEND = "append"
	PUT    = "put"
	ILLAGM = "illegal argument"
	HUMEN  = `Commands:
	"get k"
	"append k v"
	"put k v"
`
)

func main() {
	config := getcClientConfig()
	num := len(config.ClientEnd)
	if (num & 1) == 0 {
		panic("总服务器数量必须为单数")
	}

	clientEnds := getClientEnds(config)

	clerk := kv.MakeClerk(clientEnds)

	commands := []*Command{
		{
			Name: GET,
			Num:  2,
			Action: func(args []string) {
				println(clerk.Get(args[1]))
			},
		},
		{
			Name: APPEND,
			Num:  3,
			Action: func(args []string) {
				clerk.Append(args[1], args[2])
			},
		},
		{
			Name: PUT,
			Num:  3,
			Action: func(args []string) {
				clerk.Put(args[1], args[2])
			},
		},
	}

	println(HUMEN)
	for line := readLine(); !strings.EqualFold(line, EXIT); {
		args := parseArgs(line)
		if len(args) > 0 {
			name := args[0]
			do := false
			for _, command := range commands {
				if command.Name != name {
					continue
				}

				do = true
				if len(args) != command.Num {
					println(ILLAGM + HUMEN)
					continue
				}
				command.Action(args)
			}

			if !do {
				println(HUMEN)
			}
		}

		line = readLine()
	}
}

func parseArgs(line string) []string {
	argsT := strings.Split(line, " ")
	args := make([]string, 0)
	for _, str := range argsT {
		if !strings.EqualFold(str, " ") && !strings.EqualFold(str, "") {
			args = append(args, str)
		}
	}
	return args
}

func readLine() string {
	reader := bufio.NewReader(os.Stdin)
	print(TIP)
	answer, _, err := reader.ReadLine()
	if err != nil {
		log.Fatal(err)
	}
	return strings.TrimSpace(string(answer))
}

func getClientEnds(config *ClientConfig) []*rpcutil.ClientEnd {
	clientEnds := make([]*rpcutil.ClientEnd, 0)
	for _, end := range config.ClientEnd {
		address := end.Ip + ":" + end.Port
		client := rpcutil.TryConnect(address)

		ce := &rpcutil.ClientEnd{
			Addr:   address,
			Client: client,
		}

		clientEnds = append(clientEnds, ce)
	}
	return clientEnds
}

func getcClientConfig() *ClientConfig {
	var path string
	if len(os.Args) == 1 {
		path = "config/client.yml"
	} else {
		path = os.Args[1]
	}
	cfgbt, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}

	config := &ClientConfig{}
	err = yaml.Unmarshal(cfgbt, config)
	if err != nil {
		panic(err)
	}
	return config
}
