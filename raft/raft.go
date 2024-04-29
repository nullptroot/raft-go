package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"labgob"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// 日志项
type LogEntry struct {
	Command interface{}
	Term    int
}

// 当前角色
const ROLE_LEADER = "Leader"
const ROLE_FOLLOWER = "Follower"
const ROLE_CANDIDATES = "Candidates"

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 所有服务器，持久化状态（lab-2A不要求持久化）
	currentTerm int        // 见过的最大任期
	votedFor    int        // 记录在currentTerm任期投票给谁了
	log         []LogEntry // 操作日志

	// 所有服务器，易失状态
	commitIndex int // 已知的最大已提交索引
	lastApplied int // 当前应用到状态机的索引

	// 仅Leader，易失状态（成为leader时重置）
	nextIndex  []int //	每个follower的log同步起点索引（初始为leader log的最后一项）
	matchIndex []int // 每个follower的log同步进度（初始为0），和nextIndex强关联

	// 所有服务器，选举相关状态
	role              string    // 身份
	leaderId          int       // leader的id
	lastActiveTime    time.Time // 上次活跃时间（刷新时机：收到leader心跳、给其他candidates投票、请求其他节点投票）
	lastBroadcastTime time.Time // 作为leader，上次的广播时间
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.role == ROLE_LEADER
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// 持久化功能，记录了当前raft的term，是否投票了，日志信息
// 主要保证一个term只有一个leader，一个term只有一次投票
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	DPrintf("RaftNode[%d] persist starts, currentTerm[%d] voteFor[%d] log[%v]", rf.me, rf.currentTerm, rf.votedFor, rf.log)
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
// 从文件中获得持久化的信息。
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// 供leader回退nextIndex使用
	ConflictIndex int // 冲突的index
	ConflictTerm  int // 冲突的term
}

// example RequestVote RPC handler.
// 候选者向远端raft发起的rpc调用，请求给自己投票
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 先设置响应数据的初始状态
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	DPrintf("RaftNode[%d] Handle RequestVote, CandidatesId[%d] Term[%d] CurrentTerm[%d] LastLogIndex[%d] LastLogTerm[%d] votedFor[%d]",
		rf.me, args.CandidateId, args.Term, rf.currentTerm, args.LastLogIndex, args.LastLogTerm, rf.votedFor)
	defer func() {
		DPrintf("RaftNode[%d] Return RequestVote, CandidatesId[%d] Term[%d] currentTerm[%d] VoteGranted[%v] ", rf.me, args.CandidateId,
			args.Term, rf.currentTerm, reply.VoteGranted)
	}()

	// 任期不如我大，拒绝投票
	// term都不比我大，直接拒绝
	if args.Term < rf.currentTerm {
		return
	}

	// 发现更大的任期，则转为该任期的follower
	// 遇见更大的term，那么本节点之前term的投票信息
	// 就直接初始化了，后面会给大的term投票
	// 注意这里没有等于的情况
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = ROLE_FOLLOWER
		rf.votedFor = -1
		rf.leaderId = -1
		// 继续向下走，进行投票
	}

	// 每个任期，只能投票给1人
	// 没有投票或者已经投给自己了
	// 已经投给自己了有点奇怪，应该是网络重传的情况吧，网络环境错综复杂
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// 获取当前raft节点的最新日志term
		lastLogTerm := 0
		if len(rf.log) != 0 {
			lastLogTerm = rf.log[len(rf.log)-1].Term
		}
		// 这里坑了好久，一定要严格遵守论文的逻辑，另外log长度一样也是可以给对方投票的
		// candidate的日志必须比我的新
		// 1, 最后一条log，任期大的更新
		// 2，任期相同, 更长的log则更新
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= len(rf.log)) {
			// 下面的就是成功给其投票的rpc响应
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.lastActiveTime = time.Now() // 为其他人投票，那么重置自己的下次投票时间
		}
	}
	rf.persist()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("RaftNode[%d] Handle AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] role=[%s] logIndex[%d] prevLogIndex[%d] prevLogTerm[%d] commitIndex[%d] Entries[%v]",
		rf.me, rf.leaderId, args.Term, rf.currentTerm, rf.role, len(rf.log), args.PrevLogIndex, args.PrevLogTerm, rf.commitIndex, args.Entries)

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	defer func() {
		DPrintf("RaftNode[%d] Return AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] role=[%s] logIndex[%d] prevLogIndex[%d] prevLogTerm[%d] Success[%v] commitIndex[%d] log[%v] ConflictIndex[%d]",
			rf.me, rf.leaderId, args.Term, rf.currentTerm, rf.role, len(rf.log), args.PrevLogIndex, args.PrevLogTerm, reply.Success, rf.commitIndex, rf.log, reply.ConflictIndex)
	}()
	// 小于我我还听你的干啥？
	if args.Term < rf.currentTerm {
		return
	}

	// 发现更大的任期，则转为该任期的follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = ROLE_FOLLOWER
		rf.votedFor = -1
		rf.leaderId = -1
		rf.persist()
		// 继续向下走
	}

	// 认识新的leader
	rf.leaderId = args.LeaderId
	// 刷新活跃时间
	rf.lastActiveTime = time.Now()

	// appendEntries RPC , receiver 2)
	// 如果本地没有前一个日志的话，那么false
	// 相当于日志条目都不够啊，不允许有空洞
	if len(rf.log) < args.PrevLogIndex {
		reply.ConflictIndex = len(rf.log)
		return
	}
	// 如果本地有前一个日志的话，那么term必须相同，否则false
	// 前一日志条目term和leader前一日志term不对，找到该冲突term的第一个日志index
	// 返回失败了
	if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex-1].Term
		// 找到冲突term的首次出现位置，最差就是PrevLogIndex
		for index := 1; index <= args.PrevLogIndex; index++ {
			if rf.log[index-1].Term == reply.ConflictTerm {
				reply.ConflictIndex = index
				break
			}
		}
		return
	}

	// 保存日志
	for i, logEntry := range args.Entries {
		index := args.PrevLogIndex + i + 1
		// 若是新日志，直接添加上即可
		if index > len(rf.log) {
			rf.log = append(rf.log, logEntry)
		} else { // 重叠部分
			// 有冲突的多余部分，直接截断，后续添加正确的日志即可
			if rf.log[index-1].Term != logEntry.Term {
				rf.log = rf.log[:index-1]         // 删除当前以及后续所有log
				rf.log = append(rf.log, logEntry) // 把新log加入进来
			} // term一样啥也不用做，继续向后比对Log
		}
	}
	rf.persist()

	// 更新提交下标
	// 根据leader已经提交的index更新，如果自己的日志条目少于
	// leader已经提交的，那么就是小者
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if len(rf.log) < rf.commitIndex {
			rf.commitIndex = len(rf.log)
		}
	}
	reply.Success = true
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 只有leader才能写入
	if rf.role != ROLE_LEADER {
		return -1, -1, false
	}

	logEntry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.log = append(rf.log, logEntry)
	index = len(rf.log)
	term = rf.currentTerm
	rf.persist()

	DPrintf("RaftNode[%d] Add Command, logIndex[%d] currentTerm[%d]", rf.me, index, term)
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) electionLoop() {
	for !rf.killed() {
		// 每10ms 检测一次，有没有超时，需不需要重新选举leader
		time.Sleep(10 * time.Millisecond)

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			now := time.Now()
			// 这里超时的时间是随机的，因为如果同一时间超时，那么
			// 可能会出现选举票数相对平均，导致谁也不能晋升为leader
			// 下面是200~350ms之间
			timeout := time.Duration(200+rand.Int31n(150)) * time.Millisecond // 超时随机化
			elapses := now.Sub(rf.lastActiveTime)
			// follower -> candidates
			// 如果当前是follower，并且已经过了超时时间，那么就会变为candidate
			// 这是从follower晋升来的
			if rf.role == ROLE_FOLLOWER {
				if elapses >= timeout {
					rf.role = ROLE_CANDIDATES
					DPrintf("RaftNode[%d] Follower -> Candidate", rf.me)
				}
			}
			// 请求vote
			// 还有一种情况，就是本身就是candidate，这个可能是因为
			// 上一个term没有选出leader，因此重新投票的情况
			// 进入拉选票阶段
			if rf.role == ROLE_CANDIDATES && elapses >= timeout {
				// 重置leader心跳包定时器
				rf.lastActiveTime = now // 重置下次选举时间

				rf.currentTerm += 1 // 发起新任期
				rf.votedFor = rf.me // 该任期投了自己
				// 需要持久化，因为这些状态已经改变了
				rf.persist()

				// 初始化请求投票的请求参数
				// 包括当前的term、候选者、和最新日志的数量
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: len(rf.log),
				}
				// 当前raft有日志时，需要带上最近日志的term
				if len(rf.log) != 0 {
					args.LastLogTerm = rf.log[len(rf.log)-1].Term
				}
				// 解锁是因为，下面不会更改raft的状态信息了
				// 细化一下锁粒度
				rf.mu.Unlock()

				DPrintf("RaftNode[%d] RequestVote starts, Term[%d] LastLogIndex[%d] LastLogTerm[%d]", rf.me, args.Term,
					args.LastLogIndex, args.LastLogTerm)
				// 封装一个数据结构，来方便后续的票数统计
				type VoteResult struct {
					peerId int
					resp   *RequestVoteReply
				}
				// 自身发出的选票，先给自己投一票
				voteCount := 1   // 收到投票个数（先给自己投1票）
				finishCount := 1 // 收到应答个数
				// 来传递请求的结构的管道
				voteResultChan := make(chan *VoteResult, len(rf.peers))
				// 为每个远端的raft请求，让其给自己投票
				for peerId := 0; peerId < len(rf.peers); peerId++ {
					go func(id int) {
						if id == rf.me {
							return
						}
						// 接受respond的结构
						resp := RequestVoteReply{}
						// 发起rpc请求，请求结果通过管道发送给主go程
						// 请求成功，直接把结果返回即可，失败返回一个nil
						if ok := rf.sendRequestVote(id, &args, &resp); ok {
							voteResultChan <- &VoteResult{peerId: id, resp: &resp}
						} else {
							voteResultChan <- &VoteResult{peerId: id, resp: nil}
						}
					}(peerId)
				}
				// 请求的响应，可能有更大的term，此时自己就需要更改自己的term了
				maxTerm := 0
				// 死循环的等待前面发送的rpc请求
				// 结束条件有两个，首先是所有节点都已经给出响应了（rpc调用失败的也算）
				// 还有一个就是票数过半了
				for {
					select {
					case voteResult := <-voteResultChan:
						// 不管rpc是否调用成功，finishCount都会累加·
						finishCount += 1
						// rpc调用成功时，开始统计票数，并计算最大term
						if voteResult.resp != nil {
							// 得到票数就累加已获得票数
							if voteResult.resp.VoteGranted {
								voteCount += 1
							}
							// 更新maxTerm
							if voteResult.resp.Term > maxTerm {
								maxTerm = voteResult.resp.Term
							}
						}
						// 得到大多数vote后，立即离开
						if finishCount == len(rf.peers) || voteCount > len(rf.peers)/2 {
							goto VOTE_END
						}
					}
				}
				// 根据投票结果来看是否选举成功
			VOTE_END:
				rf.mu.Lock()
				defer func() {
					DPrintf("RaftNode[%d] RequestVote ends, finishCount[%d] voteCount[%d] Role[%s] maxTerm[%d] currentTerm[%d]", rf.me, finishCount, voteCount,
						rf.role, maxTerm, rf.currentTerm)
				}()
				// 如果角色改变了，则忽略本轮投票结果
				// 这种情况就是，当前节点正在选举，还没统计票数呢
				// 集群中已经选出leader了，并且发送了心跳包，然后
				// 当前节点收到心跳包后，降为follower，不用统计了
				// 已经有leader了
				if rf.role != ROLE_CANDIDATES {
					return
				}
				// 发现了更高的任期，切回follower
				// 发现更改的任期后，直接初始化投票的一些
				// 信息，因为上次的term无用了，需要重新投票了
				if maxTerm > rf.currentTerm {
					rf.role = ROLE_FOLLOWER
					rf.leaderId = -1
					rf.currentTerm = maxTerm
					rf.votedFor = -1
					rf.persist()
					return
				}
				// 赢得大多数选票，则成为leader
				// 下面就是获得大多数的选票，晋升为leader的逻辑
				if voteCount > len(rf.peers)/2 {
					rf.role = ROLE_LEADER
					rf.leaderId = rf.me
					// 称为leader后 才会有nextIndex，并且时leader最新的日志
					// 算一种lazy思想吧，仅当时leader时，才分配这两个东西
					// 用来记录与follower同步的日志index
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						// 刚开始是leader的最新日志index，通过冲突不断回滚
						rf.nextIndex[i] = len(rf.log) + 1
						rf.matchIndex[i] = 0
					}
					// 通过设置lastBroadcastTime来立即广播心跳包
					// 下面相当于设置上次广播时间是1970年，肯定过期了
					rf.lastBroadcastTime = time.Unix(0, 0) // 令appendEntries广播立即执行
					return
				}
			}
		}()
	}
}

// lab-2A只做心跳，不考虑log同步
func (rf *Raft) appendEntriesLoop() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 只有leader才向外广播心跳
			if rf.role != ROLE_LEADER {
				return
			}

			// 100ms广播1次
			now := time.Now()
			if now.Sub(rf.lastBroadcastTime) < 100*time.Millisecond {
				return
			}
			// 更新广播时间
			rf.lastBroadcastTime = time.Now()

			// 并发RPC心跳
			// type AppendResult struct {
			// 	peerId int
			// 	resp   *AppendEntriesReply
			// }
			// 对每个follower发送心跳包/日志请求
			for peerId := 0; peerId < len(rf.peers); peerId++ {
				if peerId == rf.me {
					continue
				}
				// 组装要发送的信息
				args := AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LeaderCommit = rf.commitIndex
				// 要发送的日志条目
				args.Entries = make([]LogEntry, 0)
				// 要发送的日志的前一条日志信息
				args.PrevLogIndex = rf.nextIndex[peerId] - 1
				// 还有前一条日志的term信息
				if args.PrevLogIndex > 0 {
					args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
				}
				// 把nextIndex到最后的日志全部都append到要发送的数据中
				args.Entries = append(args.Entries, rf.log[rf.nextIndex[peerId]-1:]...)

				DPrintf("RaftNode[%d] appendEntries starts,  currentTerm[%d] peer[%d] logIndex=[%d] nextIndex[%d] matchIndex[%d] args.Entries[%d] commitIndex[%d]",
					rf.me, rf.currentTerm, peerId, len(rf.log), rf.nextIndex[peerId], rf.matchIndex[peerId], len(args.Entries), rf.commitIndex)
				// log相关字段在lab-2A不处理
				go func(id int, args1 *AppendEntriesArgs) {
					// DPrintf("RaftNode[%d] appendEntries starts, myTerm[%d] peerId[%d]", rf.me, args1.Term, id)
					reply := AppendEntriesReply{}
					// 发起rpc调用，让远端raft执行日志复制的操作，或者心跳包
					if ok := rf.sendAppendEntries(id, args1, &reply); ok {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						defer func() {
							DPrintf("RaftNode[%d] appendEntries ends,  currentTerm[%d]  peer[%d] logIndex=[%d] nextIndex[%d] matchIndex[%d] commitIndex[%d]",
								rf.me, rf.currentTerm, id, len(rf.log), rf.nextIndex[id], rf.matchIndex[id], rf.commitIndex)
						}()
						// 如果不是rpc前的leader状态了，那么啥也别做了
						// 有可能自己发送rpc时，集群里别的机器已经投票出新leader了
						// 任期改变了。
						if rf.currentTerm != args1.Term {
							return
						}
						// 遇见比自己大的term，就直接降级就ok了
						if reply.Term > rf.currentTerm { // 变成follower
							rf.role = ROLE_FOLLOWER
							rf.leaderId = -1
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.persist()
							return
						}
						// 因为RPC期间无锁, 可能相关状态被其他RPC修改了
						// 因此这里得根据发出RPC请求时的状态做更新，而不要直接对nextIndex和matchIndex做相对加减
						// 只有响应出现Success才会有下面的逻辑，并且args1.PrevLogIndex + len(args1.Entries) + 1
						// 都是和当前leader一样的日志
						if reply.Success { // 同步日志成功
							// 下一条要同步的日志
							rf.nextIndex[id] = args1.PrevLogIndex + len(args1.Entries) + 1
							// 远端已经复制的日志index，不是已提交嗷
							rf.matchIndex[id] = rf.nextIndex[id] - 1

							// 数字N, 让peer[i]的大多数>=N
							// peer[0]' index=2
							// peer[1]' index=2
							// peer[2]' index=1
							// 1,2,2
							// 更新commitIndex, 就是找中位数
							// 应该是模拟寻找大多数  好像有点牛逼嗷
							// 只有成功的请求才会走到这里来对commitIndex进行更新
							// 中位数既是大多数commit的index，有点技巧嗷
							sortedMatchIndex := make([]int, 0)
							// 本leader已经复制的日志条目
							sortedMatchIndex = append(sortedMatchIndex, len(rf.log))
							for i := 0; i < len(rf.peers); i++ {
								if i == rf.me {
									continue
								}
								// 获取每个远端raft已经复制的条目
								// 能到这里，说明远端raft和本leader的日志条目是不冲突的
								// 可能数量不同，但是肯定不冲突，所有日志都是正确的
								sortedMatchIndex = append(sortedMatchIndex, rf.matchIndex[i])
							}
							// 排序
							sort.Ints(sortedMatchIndex)
							// 寻找中位数，就是大多数
							newCommitIndex := sortedMatchIndex[len(rf.peers)/2]
							// 如果newCommitIndex < rf.commitIndex 那说明rf.commitIndex之前已经到达大多数了
							if newCommitIndex > rf.commitIndex && rf.log[newCommitIndex-1].Term == rf.currentTerm {
								rf.commitIndex = newCommitIndex
							}
							// 失败就只有两种情况
							// 1. 一种就是有冲突的term
							// 2. 一种远端的raft日志条目都不够
						} else { // 失败了就需要回退nextIndex了
							// 回退优化，参考：https://thesquareplanet.com/blog/students-guide-to-raft/#an-aside-on-optimizations
							nextIndexBefore := rf.nextIndex[id] // 仅为打印log
							// 远端raft的前一条日志和本leader前一条日志term不同
							// 1. 一种就是有冲突的term
							if reply.ConflictTerm != -1 { // follower的prevLogIndex位置term不同
								conflictTermIndex := -1
								// 看看当前leader有没有冲突的term的日志
								for index := args1.PrevLogIndex; index >= 1; index-- { // 找最后一个conflictTerm
									if rf.log[index-1].Term == reply.ConflictTerm {
										conflictTermIndex = index
										break
									}
								}
								// 如果存在远端raft起冲突的term，那么就直接使用当前leader该term的最后一条
								// 日志进行尝试，可能不对，不对再回滚即可
								if conflictTermIndex != -1 { // leader也存在冲突term的日志，则从term最后一次出现之后的日志开始尝试同步，因为leader/follower可能在该term的日志有部分相同
									rf.nextIndex[id] = conflictTermIndex + 1
									// leader中没有term相关的日志，那么把远端该term的第一条日志作为下一个，也是可能，说不定前一个term仍然有问题
								} else { // leader并没有term的日志，那么把follower日志中该term首次出现的位置作为尝试同步的位置，即截断follower在此term的所有日志
									rf.nextIndex[id] = reply.ConflictIndex
								}
								// 2. 一种远端的raft日志条目都不够
							} else { // follower的prevLogIndex位置没有日志
								rf.nextIndex[id] = reply.ConflictIndex + 1
							}
							DPrintf("RaftNode[%d] back-off nextIndex, peer[%d] nextIndexBefore[%d] nextIndex[%d]", rf.me, id, nextIndexBefore, rf.nextIndex[id])
						}
					}
				}(peerId, &args)
			}
		}()
	}
}

// 这个就是把command应用到上层应用
func (rf *Raft) applyLogLoop(applyCh chan ApplyMsg) {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// 不断的把已提交的日志应用到应用层
			if rf.commitIndex > rf.lastApplied {
				rf.lastApplied += 1
				// 通过下面的通道通信 应用层通过通道获取数据信息。
				applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log[rf.lastApplied-1].Command,
					CommandIndex: rf.lastApplied,
				}
				DPrintf("RaftNode[%d] applyLog, currentTerm[%d] lastApplied[%d] commitIndex[%d]", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
			}
		}()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	// 初始化raft节点
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = ROLE_FOLLOWER
	rf.leaderId = -1
	rf.votedFor = -1
	rf.lastActiveTime = time.Now()

	// initialize from state persisted before a crash
	// 持久化的东西  这个是从本地读取状态启动，因为可能是因为宕机重启
	rf.readPersist(persister.ReadRaftState())
	DPrintf("RaftNode[%d] Make again", rf.me)

	// election逻辑
	// 领导者选举的主要逻辑
	go rf.electionLoop()
	// leader逻辑
	// leader发送心跳包和发送日志的主要逻辑
	go rf.appendEntriesLoop()
	// apply逻辑
	// 把同步的日志应用到应用层的逻辑
	go rf.applyLogLoop(applyCh)

	DPrintf("Raftnode[%d]启动", me)

	return rf
}
