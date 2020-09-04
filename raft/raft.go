package raft


import (
	"kvuR/labgob"
	"kvuR/rpc"
	"bytes"
	"log"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

type CommandState struct {
	Term  int
	Index int
}

const (
	FOLLOWER    = 0
	CANDIDATE   = 1
	LEADER      = 2
	CopyEntries = 3
	HeartBeat   = 4
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*rpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// state a Raft server must maintain.
	currentTerm int        // 服务器最后一次知道的任期号（初始化为 0，持续递增）
	votedFor    int        // 在当前获得选票的候选人的 Id
	logs        []LogEntry // 日志条目集；每一个条目包含一个用户状态机执行的指令，和收到时的任期号

	commitIndex int // 已知的最大的已经被提交的日志条目的索引值
	lastApplied int // 最后被应用到状态机的日志条目索引值（初始化为 0，持续递增）

	nextIndex  []int // 对于每一个服务器，需要发送给他的下一个日志条目的索引值（初始化为领导人最后索引值加一）
	matchIndex []int // 对于每一个服务器，已经复制给他的日志的最高索引值

	identity     int
	peersLen     int
	hbCnt        int
	applyCh      chan ApplyMsg
	doAppendCh   chan int
	applyCmdLogs map[interface{}]*CommandState
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("%v: 获取 %v 的状态", rf.currentTerm, rf.me)
	term = rf.currentTerm
	isleader = rf.identity == LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	func() {
		rf.mu.Lock()
		rf.mu.Unlock()
		e.Encode(rf.votedFor)
		e.Encode(rf.currentTerm)
		e.Encode(rf.logs)
		//DPrintf("%v: %v 持久化了 votedFor=%v, logs 的最后一个是 %v",
		//	rf.currentTerm, rf.me, rf.votedFor, rf.logs[len(rf.logs) - 1])
	}()
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var votedFor, currentTerm int
	var logs []LogEntry
	err1 := d.Decode(&votedFor)
	err2 := d.Decode(&currentTerm)
	err3 := d.Decode(&logs)
	if err1 != nil ||
		err2 != nil ||
		err3 != nil {
		log.Fatalf("%v 恢复失败 err1=%v, err2=%v, err3=%v", rf.me, err1, err2, err3)
	} else {
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
		rf.logs = logs
		for i := range rf.logs {
			logEntry := rf.logs[i]
			rf.applyCmdLogs[logEntry.Command] = &CommandState{
				Term:  logEntry.Term,
				Index: logEntry.Index,
			}
		}
		//DPrintf("%v 恢复了 votedFor=%v, term=%v, logs=%v", rf.me, votedFor, currentTerm, logs)
	}
}

type AppendEntriesArgs struct {
	Term         int        // 领导人的任期号
	LeaderId     int        // 领导人的 Id，以便于跟随者重定向请求
	PrevLogIndex int        // 新的日志条目紧随之前的索引值
	PrevLogTerm  int        // PrevLogIndex 条目的任期号
	Entries      []LogEntry // 准备存储的日志条目（表示心跳时为空；一次性发送多个是为了提高效率）
	LeaderCommit int        // 领导人已经提交的日志的索引值
}

type AppendEntriesReply struct {
	Term         int        //当前的任期号，用于领导人去更新自己
	Success      bool       // 跟随者包含了匹配上 PrevLogIndex 和 PrevLogTerm 的日志时为真
	PrevLogIndex int
}

type RequestVoteArgs struct {
	Term         int // 候选人的任期号
	CandidateId  int // 请求选票的候选人的 Id
	LastLogIndex int // 候选人的最后日志条目的索引值
	LastLogTerm  int // 候选人最后日志条目的任期号
}

type RequestVoteReply struct {
	Term        int  // 当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool // 候选人赢得了此张选票时为真
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		DPrintf("%v: %v 认为 %v 的投票请求 {term=%v, lastIdx=%v, lastTerm=%v} 过时了",
			rf.currentTerm, rf.me, args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
		return
	}

	if args.Term > rf.currentTerm {
		DPrintf("%v: %v 接收到投票请求后发现自己过时，变回追随者，新的任期为 %v", rf.currentTerm, rf.me, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.identity = FOLLOWER
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLog := rf.logs[len(rf.logs)-1]
		if args.LastLogTerm > lastLog.Term {
			rf.identity = FOLLOWER
			rf.votedFor = args.CandidateId
			rf.hbCnt++
			reply.VoteGranted = true
		} else if args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index {
			rf.identity = FOLLOWER
			rf.votedFor = args.CandidateId
			rf.hbCnt++
			reply.VoteGranted = true
		}
	}
	go rf.persist()
	DPrintf("%v: %v 对 %v 的投票 {term=%v, lastIdx=%v, lastTerm=%v} 结果为 %v, votedFor=%v",
		rf.currentTerm, rf.me, args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm, reply.VoteGranted, rf.votedFor)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.PrevLogIndex = args.PrevLogIndex
	if args.Term > rf.currentTerm {
		DPrintf("%v: %v 收到条目增加请求后发现自己过时，变回追随者，新任期为 %v", rf.currentTerm, rf.me, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.identity = FOLLOWER
	}

	l1 := len(rf.logs)
	if args.Term < rf.currentTerm {
		if len(args.Entries) > 0 {
			DPrintf("%v: %v 接收到 %v 的复制请求 {{%v ~ %v} term=%v leaderCommit=%v}，结果为：已过时",
				rf.currentTerm, rf.me, args.LeaderId, args.Entries[0], args.Entries[len(args.Entries)-1],
				args.Term, args.LeaderCommit)
		} else {
			DPrintf("%v: %v 接收到 %v 的复制请求 {term=%v leaderCommit=%v}，结果为：已过时",
				rf.currentTerm, rf.me, args.LeaderId, args.Term, args.LeaderCommit)
		}
		return
	}
	if args.PrevLogIndex >= l1 || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		if len(args.Entries) > 0 {
			DPrintf("%v: %v 接收到 %v 的复制请求 {{%v ~ %v} term=%v leaderCommit=%v}，结果为：不匹配",
				rf.currentTerm, rf.me, args.LeaderId, args.Entries[0], args.Entries[len(args.Entries)-1],
				args.Term, args.LeaderCommit)
		} else {
			DPrintf("%v: %v 接收到 %v 的复制请求 {term=%v leaderCommit=%v}，结果为：不匹配",
				rf.currentTerm, rf.me, args.LeaderId, args.Term, args.LeaderCommit)
		}
		//DPrintf("%v: %v 接收到 %v 的增加条目请求(%v)，结果为 %v", rf.currentTerm, rf.me, args.LeaderId, *args, reply.Success)
		if args.PrevLogIndex >= l1 {
			reply.PrevLogIndex = l1
			return
		}

		i := args.PrevLogIndex
		term := rf.logs[i].Term
		for i--; i >= 0 && rf.logs[i].Term == term; i-- {
		}
		reply.PrevLogIndex = i + 1
		return
	}

	if rf.votedFor == -1 {
		rf.votedFor = args.LeaderCommit
	}

	var deleteLogEntries []LogEntry
	idx1 := args.PrevLogIndex + 1
	idx2 := len(args.Entries) - 1
	for idx1 < l1 && idx2 >= 0 {
		log1 := &rf.logs[idx1]
		log2 := args.Entries[idx2]
		if log1.Term != log2.Term || log1.Index != log2.Index {
			deleteLogEntries = rf.logs[idx1:]
			rf.logs = rf.logs[:idx1]
			break
		}
		idx1++
		idx2--
	}
	for i := 0; i < len(deleteLogEntries); i++ {
		delete(rf.applyCmdLogs, deleteLogEntries[i])
	}
	for idx2 >= 0 {
		logEntry := args.Entries[idx2]
		rf.logs = append(rf.logs, logEntry)
		rf.applyCmdLogs[logEntry.Command] = &CommandState{
			Term:  logEntry.Term,
			Index: logEntry.Index,
		}
		idx2--
	}
	if rf.commitIndex < args.LeaderCommit {
		idx := len(rf.logs) - 1
		if args.LeaderCommit <= idx {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = idx
		}
		go rf.apply()
	}

	rf.hbCnt++
	reply.Success = true
	go rf.persist()
	if len(args.Entries) > 0 {
		DPrintf("%v: %v 接收到 %v 的复制请求 {{%v ~ %v} term=%v leaderCommit=%v}，结果为：成功",
			rf.currentTerm, rf.me, args.LeaderId, args.Entries[0], args.Entries[len(args.Entries)-1],
			args.Term, args.LeaderCommit)
	} else {
		DPrintf("%v: %v 接收到 %v 的复制请求 {term=%v leaderCommit=%v}，结果为：成功",
			rf.currentTerm, rf.me, args.LeaderId, args.Term, args.LeaderCommit)
	}
	//DPrintf("%v: %v 接收到 %v 的增加条目(%v)请求，结果为 %v", rf.currentTerm, rf.me, args.LeaderId, *args, reply.Success)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = !rf.killed() && rf.identity == LEADER
	term = rf.currentTerm
	if isLeader {
		if commandState, has := rf.applyCmdLogs[command]; has {
			return commandState.Index, commandState.Term, isLeader
		}

		index = len(rf.logs)
		logEntry := LogEntry{
			Command: command,
			Term:    term,
			Index:   index,
		}
		rf.logs = append(rf.logs, logEntry)
		rf.applyCmdLogs[command] = &CommandState{
			Term:  term,
			Index: index,
		}
		go rf.persist()
		//go rf.sendLogEntry(CopyEntries)
		go func() {
			rf.doAppendCh <- CopyEntries
		}()
		DPrintf("-----%v 是领导，增加了新的条目为 {%v %v %v}", rf.me, command, term, index)
	}

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	DPrintf("----- %v 结束 -----", rf.me)
	func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		DPrintf("%v: %v 的日志内容：%v", rf.currentTerm, rf.me, rf.logs)
	}()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) sendLogEntry(flag int) {
	rf.mu.Lock()
	if rf.identity != LEADER {
		rf.mu.Unlock()
		return
	}

	argsTemplate := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}

	loglen := len(rf.logs)
	//if flag == CopyEntries {
	DPrintf("%v: %v 开始复制，最后一个条目为 %v，最后提交的索引为 %v",
		rf.currentTerm, rf.me, rf.logs[loglen-1], rf.commitIndex)
	//}
	rf.mu.Unlock()

	resultCh := make(chan int, rf.peersLen)
	wg := sync.WaitGroup{}
	wg.Add(1)
	defer wg.Done()
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			args := argsTemplate
			preIdx := func() int {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				nextIndex := loglen - 1
				for ; nextIndex >= rf.nextIndex[i]; nextIndex-- {
					args.Entries = append(args.Entries, rf.logs[nextIndex])
				}
				return nextIndex
			}()

			for preIdx >= 0 {
				rf.mu.Lock()
				if rf.identity != LEADER || preIdx >= len(rf.logs) {
					rf.mu.Unlock()
					resultCh <- -2
					break
				}
				args.PrevLogIndex = preIdx
				args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
				rf.mu.Unlock()

				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, &args, &reply)
				if !ok {
					//DPrintf("%v: %v 向 %v 发送请求 {%v commit=%v perTerm=%v perIdx=%v } 时无响应",
					//	lcterm, rf.me, i, args.Entries, args.LeaderCommit, args.PrevLogTerm, args.PrevLogIndex)
					resultCh <- -2
					break
				}

				if reply.Term > args.Term {
					resultCh <- reply.Term
					break
				}

				if reply.Success {
					rf.mu.Lock()
					rf.nextIndex[i] = loglen
					rf.mu.Unlock()
					resultCh <- -1
					/*
						if len(args.Entries) > 0 {
							DPrintf("%v: %v 向 %v 发送请求 {{%v ~ %v} commit=%v perTerm=%v }，复制成功",
								args.Term, rf.me, i, args.Entries[0], args.Entries[len(args.Entries) - 1], args.LeaderCommit, args.PrevLogTerm)
						} else {
							DPrintf("%v: %v 向 %v 发送请求 {{} commit=%v perTerm=%v }，复制成功",
								args.Term, rf.me, i, args.LeaderCommit, args.PrevLogTerm)
						}
					*/
					break
				} else {
					/*
						if len(args.Entries) > 0 {
							DPrintf("%v: %v 向 %v 发送请求 {{%v ~ %v} commit=%v perTerm=%v}，复制失败",
								args.Term, rf.me, i, args.Entries[0], args.Entries[len(args.Entries) - 1], args.LeaderCommit, args.PrevLogTerm)
						} else {
							DPrintf("%v: %v 向 %v 发送请求 {{} commit=%v perTerm=%v }，复制失败",
								args.Term, rf.me, i, args.LeaderCommit, args.PrevLogTerm)
						}
					*/
					rf.mu.Lock()
					if preIdx >= len(rf.logs) {
						rf.mu.Unlock()
						break
					}
					rf.nextIndex[i] = reply.PrevLogIndex
					for ; preIdx >= reply.PrevLogIndex; preIdx-- {
						args.Entries = append(args.Entries, rf.logs[preIdx])
					}
					rf.mu.Unlock()
				}
			}
		}(i)
	}

	grantedCount := 1
	notgrantedCount := 0
	tgt := rf.peersLen / 2
	for finish := 1; finish < rf.peersLen; finish++ {
		result := <-resultCh

		rf.mu.Lock()
		if rf.identity != LEADER {
			rf.mu.Unlock()
			break
		}
		if rf.currentTerm != argsTemplate.Term {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		if result == -1 {
			grantedCount++
			if grantedCount > tgt {
				func() {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					commit := loglen - 1
					if rf.identity == LEADER && commit < len(rf.logs) && commit > rf.commitIndex {
						rf.commitIndex = commit
						go rf.apply()
						//if flag == CopyEntries {
						DPrintf("%v: %v 提交成功，提交的最大的索引为 %v，最后复制过去的是 %v",
							argsTemplate.Term, rf.me, rf.commitIndex, rf.logs[commit])
						//}
					}
				}()
				break
			}
		} else if result == -2 {
			notgrantedCount++
			if notgrantedCount > tgt {
				//if flag == CopyEntries {
				DPrintf("%v: %v 提交失败，准备提交的索引为 %v，退回追随者", argsTemplate.Term, rf.me, loglen-1)
				//}
				func() {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					rf.identity = FOLLOWER
				}()
				break
			}
		} else if result > argsTemplate.Term {
			func() {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm < result {
					rf.currentTerm = result
					rf.votedFor = -1
					rf.identity = FOLLOWER
					DPrintf("%v: %v 收到条目增加响应后发现自己过时，变成追随者，新任期为 %v",
						argsTemplate.Term, rf.me, rf.currentTerm)
				}
			}()
			break
		} else {
			panic("出现一个意外的值： result=" + string(result))
		}
	}
}

func (rf *Raft) setToFollower() {
	//DPrintf("%v: 将 %v 变为追随者", rf.currentTerm, rf.me)
	rf.identity = FOLLOWER
}

func (rf *Raft) apply() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex < rf.lastApplied {
		log.Fatalf("%v: %v 调用 apply()： commitIndex(%v) < lastApplied(%v)",
			rf.currentTerm, rf.me, rf.currentTerm, rf.lastApplied)
	}
	if rf.commitIndex == rf.lastApplied {
		return
	}
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		logEntry := rf.logs[rf.lastApplied]
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      logEntry.Command,
			CommandIndex: logEntry.Index,
		}
		//DPrintf("%v: %v 应用第 %v 个, 命令为 %v", rf.currentTerm, rf.me, rf.lastApplied, logEntry.Command)
	}
	DPrintf("%v: %v 最后应用的是 %v, logs 里最后一个是 %v",
		rf.currentTerm, rf.me, rf.logs[rf.lastApplied], rf.logs[len(rf.logs)-1])
}

func randomTimeout(min, max int) int {
	return rand.Intn(max-min) + min
}

func Make(peers []*rpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	DPrintf("----- %v 开始 -----", rf.me)
	rf.votedFor = -1
	rf.peersLen = len(peers)
	rf.logs = append(rf.logs, LogEntry{
		Command: "start",
		Term:    0,
		Index:   0,
	})
	rf.setToFollower()
	rf.commitIndex = 0
	rf.nextIndex = make([]int, rf.peersLen)
	rf.matchIndex = make([]int, rf.peersLen)
	rf.applyCh = applyCh
	rf.doAppendCh = make(chan int, 256)
	rf.applyCmdLogs = make(map[interface{}]*CommandState)
	rand.Seed(time.Now().UnixNano())

	go func() {
		for {
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			switch rf.identity {
			case FOLLOWER:
				oldCnt := rf.hbCnt
				rf.mu.Unlock()
				timeout := time.Duration(randomTimeout(700, 1000)) * time.Millisecond
				time.Sleep(timeout)
				func() {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.hbCnt == oldCnt {
						rf.identity = CANDIDATE
						rf.currentTerm++
						rf.votedFor = rf.me
						go rf.persist()
						DPrintf("%v: %v 的心跳超时 (%v)", rf.currentTerm, rf.me, timeout)
					}
				}()
			case CANDIDATE:
				DPrintf("%v: %v 开始竞选", rf.currentTerm, rf.me)
				rf.mu.Unlock()

				wonCh := make(chan int, 2)
				wg := sync.WaitGroup{}
				wg.Add(1)
				go func() {
					wg.Wait()
					close(wonCh)
				}()

				wg.Add(1)
				go rf.goFuncDoElect(wonCh, &wg)

				timeout := time.Duration(randomTimeout(3000, 3400)) * time.Millisecond
				wg.Add(1)
				go func() {
					time.Sleep(timeout)
					wonCh <- 2
					wg.Done()
				}()

				res := <-wonCh
				if 2 == res {
					DPrintf("%v: %v 竞选超时 (%v)", rf.currentTerm, rf.me, timeout)
					rf.mu.Lock()
					rf.votedFor = rf.me
					rf.currentTerm++
					rf.mu.Unlock()
					go rf.persist()
				}
				wg.Done()
			default:
				rf.mu.Unlock()
				//rf.sendLogEntry(HeartBeat)
				rf.doAppendCh <- HeartBeat
				time.Sleep(time.Millisecond * 100)
			}
		}
	}()

	go func() {
		for {
			if rf.killed() {
				return
			}

			rf.sendLogEntry(<-rf.doAppendCh)
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) goFuncDoElect(wonCh chan int, wgp *sync.WaitGroup) {
	defer wgp.Done()

	args := &RequestVoteArgs{}
	args.CandidateId = rf.me

	rf.mu.Lock()
	lcterm := rf.currentTerm
	args.Term = rf.currentTerm
	args.LastLogIndex = len(rf.logs) - 1
	args.LastLogTerm = rf.logs[args.LastLogIndex].Term
	rf.mu.Unlock()

	c := make(chan *RequestVoteReply, rf.peersLen)
	wg := sync.WaitGroup{}
	wg.Add(1)
	defer wg.Done()
	go func() {
		wg.Wait()
		close(c)
	}()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(i, args, &reply)
			if !ok {
				c <- nil
				//DPrintf("%v: %v 向 %v 发起投票请求 {term=%v, lastIdx=%v, lastTerm=%v} 超时",
				//	args.Term, rf.me, i, args.Term, args.LastLogIndex, args.LastLogTerm)
				return
			}
			c <- &reply
		}(i)
	}

	grantedCount := 1
	notgrantedCount := 0
	tgt := rf.peersLen / 2
	for finish := 1; finish < rf.peersLen; finish++ {
		reply := <-c

		rf.mu.Lock()
		if rf.currentTerm != lcterm {
			rf.mu.Unlock()
			break
		}
		if rf.identity != CANDIDATE {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		if reply == nil {
			notgrantedCount++
			continue
		}

		if reply.VoteGranted {
			grantedCount++
			if grantedCount > tgt {
				func() {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.identity != LEADER {
						rf.identity = LEADER
						nextIndex := len(rf.logs)
						for i := range rf.nextIndex {
							rf.nextIndex[i] = nextIndex
						}
						go rf.sendLogEntry(HeartBeat)
						DPrintf("%v: %v 赢得了选举，变为领导者，发送了初始心跳",
							args.Term, rf.me)
					}
				}()
				wonCh <- 1
				break
			}
		} else {
			if args.Term < reply.Term {
				func() {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.currentTerm < reply.Term {
						DPrintf("%v: %v 发送投票后发现自己过时，变回追随者", args.Term, rf.me)
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.identity = FOLLOWER
						wonCh <- 1
					}
				}()
				break
			}

			notgrantedCount++
			if notgrantedCount > tgt {
				break
			}
		}
	}
}
