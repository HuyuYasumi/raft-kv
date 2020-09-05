# kvuR

A Go implementation of Raft Algorithm & A fault-tolerant primary/backup K/V storage system that use the implementation.

## Raft

### To see the paper: [In Search of an Understandable Consensus Algorithm(Extended Version)](https://raft.github.io/raft.pdf) 

### To see the implementation: [raft](raft)

## Primary/backup K/V storage system

### Example

Run three k/v storage server instances

```shell script
go run server/kvserver.go example/config/server1.yml
go run server/kvserver.go example/config/server2.yml
go run server/kvserver.go example/config/server3.yml
```

The number of servers must be odd and greater than or equal to 3.

```yaml
# example/config/server1.yml

servers: # address list of all k/v servers
  - ip: 127.0.0.1
    port: 10001
  - ip: 127.0.0.1
    port: 10002
  - ip: 127.0.0.1
    port: 10003
me: 0 # index of my address in address list
```

You can use the simple command line client to communicate with k/v storage service

```shell script
go run cli/kvcli.go example/config/client.yml
```

```yaml
# example/config/client.yml

servers: # address list of all k/v servers
  - ip: 127.0.0.1
    port: 10001
  - ip: 127.0.0.1
    port: 10002
  - ip: 127.0.0.1
    port: 10003
```

Or use the client api

```go
package main

import (
	"fmt"
	"kvuR/kv"
)

func main() {
	clientEnds := kv.GetClientEnds("example/config/client.yml")

	clerk := kv.MakeClerk(clientEnds)

	fmt.Printf("k1=%v\n", clerk.Get("k1"))

	clerk.Put("k1", "3")
	fmt.Printf("k1=%v\n", clerk.Get("k1"))

	clerk.Append("k1", "4")
	fmt.Printf("k1=%v\n", clerk.Get("k1"))
}
```

