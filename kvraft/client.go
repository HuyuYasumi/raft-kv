package kvraft

import (
	"kvuR/rpc"
	"fmt"
	"github.com/satori/go.uuid"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*rpc.ClientEnd
	id uuid.UUID
	servlen int
	leader  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*rpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id = generateUUID()
	ck.servlen = len(servers)
	return ck
}

func (ck *Clerk) Get(key string) string {

	args := &GetArgs{
		Key:    key,
		Id:     ck.id,
		Serial: generateUUID(),
	}
	reply := &GetReply{}
	DPrintf("%v 发送 Get 请求 {Key=%v Serial=%v}",
		ck.id, key, args.Serial)
	for {
		if ok := ck.servers[ck.leader].Call(RPCGet, args, reply);
			!ok {
			//DPrintf("%v 对 服务器 %v 的 Get 请求 (Key=%v Serial=%v) 超时",
			//	ck.id, ck.leader, key, args.Serial)
			ck.leader = (ck.leader + 1) % ck.servlen
			continue
		}
		if reply.Err == OK {
			//DPrintf("%v 收到对 %v 发送的 Get 请求 {Key=%v Serial=%v} 的响应，结果为 %v",
			//	ck.id, ck.leader, key, args.Serial, reply.Value)
			return reply.Value
		} else if reply.Err == ErrNoKey {
			//DPrintf("%v 收到对 %v 发送的 Get 请求 {Key=%v Serial=%v} 的响应，结果为 ErrNoKey",
			//	ck.id, ck.leader, key, args.Serial)
			return NoKeyValue
		} else if reply.Err == ErrWrongLeader {
			ck.leader = (ck.leader + 1) % ck.servlen
			continue
		} else {
			panic(fmt.Sprintf("%v 对 服务器 %v 的 Get 请求 (Key=%v Serial=%v) 收到一条空 Err",
				ck.id, ck.leader, key, args.Serial))
		}
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Id:     ck.id,
		Serial: generateUUID(),
	}
	reply := &PutAppendReply{}
	DPrintf("%v 发送 PA 请求 {Op=%v Key=%v Value='%v' Serial=%v}",
		ck.id, op, key, value, args.Serial)
	for {
		if ok := ck.servers[ck.leader].Call(RPCPutAppend, args, reply);
			!ok {
			//DPrintf("%v 对 服务器 %v 的 PutAppend 请求 (Serial=%v Key=%v Value=%v op=%v) 超时",
			//	ck.id, ck.leader, args.Serial, key, value, op)
			ck.leader = (ck.leader + 1) % ck.servlen
			continue
		}
		if reply.Err == OK {
			//DPrintf("%v 收到对 %v 发送的 PA 请求 {Op=%v Key=%v Value='%v' Serial=%v} 的响应，结果为 OK",
			//	ck.id, ck.leader, op, key, value, args.Serial)
			return
		} else if reply.Err == ErrWrongLeader {
			ck.leader = (ck.leader + 1) % ck.servlen
			continue
		} else {
			panic(fmt.Sprintf("%v 对 服务器 %v 的 PutAppend 请求 (Serial=%v Key=%v Value=%v op=%v) 收到一条空 Err",
				ck.id, ck.leader, args.Serial, key, value, op))
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}

func generateUUID() uuid.UUID {
	id := uuid.NewV1()
	return id
}
