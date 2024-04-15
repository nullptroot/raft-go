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
	"labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// 日志条目
type LogEntry struct {
	Command interface{}
	Term    int
}

// raft的三种状态
const (
	ROLE_LEADER     = "Leader"
	ROLE_FOLLOWER   = "Follower"
	ROLE_CANDIDATES = "Candidates"
)

// import "bytes"
// import "../labgob"

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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// 持久化状态 2A不要求持久化
	currentTerm int         // 当前term号
	votedFor    int         // 当前票投给谁了  -1表示没有投票
	log         []*LogEntry //操作日志
	// 易失状态
	conmmitIndex int //已知的最大已提交索引
	lastApplied  int //当前应用到状态机上的索引

	// 仅leader 易失状态
	nextIndex  []int //每个follower的log同步起点索引 初始时是leader log的最新一项，不断回滚
	matchIndex []int // 每个follower的log同步进度，初始为0，和nextIndex强关联

	// 选举相关状态
	role              string    //状态信息
	leaderId          int       // leader的id
	lastActiveTime    time.Time // 上次活跃的时间（收到leader心跳 给其他候选者投票 请求给自己投票 都会刷新）
	lastBroadcastTime time.Time //作为leader上次广播的时间
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

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
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
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
}

// #region Vote
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选人的任期号
	CandidateId  int // 请求选票的候选人的 ID
	LastLogIndex int // 候选人的最后日志条目的索引值
	LastLogTerm  int // 候选人最后日志条目的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool // 候选人赢得了此张选票时为真
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	DPrintf("RaftNode[%d] Handle RequestVote, CandidatesId[%d] Term[%d] CurrentTerm[%d] LastLogIndex[%d] LastLogTerm[%d] votedFor[%d]",
		rf.me, args.CandidateId, args.Term, rf.currentTerm, args.LastLogIndex, args.LastLogTerm, rf.votedFor)
	defer func() {
		DPrintf("RaftNode[%d] Return RequestVote, CandidatesId[%d] VoteGranted[%v] ", rf.me, args.CandidateId, reply.VoteGranted)
	}()
	// 任期比我小  肯定不投你
	if args.Term < rf.currentTerm {
		return
	}
	// 任期比我大 当前我就是跟随者
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = ROLE_FOLLOWER
		rf.votedFor = -1
		rf.leaderId = -1
	}
	// 每个任期只能投票给一个人  下面就是投票给args的处理
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// 候选者的日志必须比我的新
		//最后一条log 任期大的更新
		//更长的log更新
		lastLogTerm := 0
		if len(rf.log) != 0 {
			lastLogTerm = rf.log[len(rf.log)-1].Term
		}
		// 请求投票的raft的最后一条日志，要大于我最后一条日志的Term，
		// 并且请求投票的raft的日志长度要大于我的日志长度我才投票，否则不投
		if args.LastLogTerm < lastLogTerm || args.LastLogIndex < len(rf.log) {
			return
		}
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastActiveTime = time.Now() //重置活跃时间
	}
	// 持久化
	rf.persist()
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

func (rf *Raft) electionLoop() {
	for !rf.killed() {
		time.Sleep(1 * time.Millisecond)
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// 当前时间
			now := time.Now()
			// 超时时间随机化  避免一直选不出leader
			timeout := time.Duration(200+rand.Int31n(150)) * time.Millisecond
			// 距上次活跃时间的时间差 elapses >= timeout时就是超时了 该重新选举leader了
			elapses := now.Sub(rf.lastActiveTime)

			// 以follower身份超时了，那么就变为候选者开始投票  这个还不能合并 因为可能本来就是候选者
			if rf.role == ROLE_FOLLOWER {
				if elapses >= timeout {
					rf.role = ROLE_CANDIDATES
					DPrintf("RaftNode[%d] Follower -> Candidate", rf.me)
				}
			}
			if rf.role == ROLE_CANDIDATES && elapses >= timeout {
				// 跟随者定时器超时了  变为候选者开始投票
				// if rf.role == ROLE_FOLLOWER && elapses >= timeout {
				// rf.role = ROLE_CANDIDATES
				// 活跃时间更为now
				rf.lastActiveTime = now
				// term加一  开始投票
				rf.currentTerm += 1
				// 投自己一票先
				rf.votedFor = rf.me
				// 持久化
				rf.persist()
				// 创建请求投票的参数
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: len(rf.log),
				}
				// 如果有日志，就把最新日志的term加上
				if len(rf.log) != 0 {
					args.LastLogTerm = rf.log[len(rf.log)-1].Term
				}
				rf.mu.Unlock()
				// 临时数据结构接受rpc的响应
				type VoteResult struct {
					peerId int
					resp   *RequestVoteReply
				}
				// 获得投票的数量
				voteCount := 1
				// 完成投票的数量（不一定投自己）
				finishCount := 1
				// 投票的结果，也就是rpc的响应，投票的结果通过chan传给主线程
				VoteResultChan := make(chan *VoteResult, len(rf.peers))
				// 遍历每一个peer 请求给自己投票
				for peerId := 0; peerId < len(rf.peers); peerId++ {
					// 并发的请求投票
					go func(id int) {
						if id == rf.me {
							return
						}
						resp := RequestVoteReply{}
						// 向id发送请求投票rpc 结果通过管道发送会主线程
						if ok := rf.sendRequestVote(id, &args, &resp); ok {
							VoteResultChan <- &VoteResult{
								peerId: id,
								resp:   &resp,
							}
						} else {
							VoteResultChan <- &VoteResult{
								peerId: id,
								resp:   nil,
							}
						}
					}(peerId)
				}
				// 最大的任期，因为peer可能比自己的任期大
				maxTerm := 0
				// 循环阻塞的等待rpc的请求响应
				for {
					select {
					case VoteResult := <-VoteResultChan:
						// 收到响应finishCount就加一
						finishCount += 1
						if VoteResult.resp != nil {
							// 投自己就累加获得的票数
							if VoteResult.resp.VoteGranted {
								voteCount += 1
							}
							// 更新最大任期
							if VoteResult.resp.Term > maxTerm {
								maxTerm = VoteResult.resp.Term
							}
						}
						// 全部投票完成或者得到半数以上的票数，就直接返回了
						if finishCount == len(rf.peers) || voteCount > len(rf.peers)/2 {
							goto VOTE_END
						}
					}
				}
			VOTE_END:
				rf.mu.Lock()
				defer func() {
					DPrintf("RaftNode[%d] RequestVote ends, finishCount[%d] voteCount[%d] Role[%s] maxTerm[%d] currentTerm[%d]", rf.me, finishCount, voteCount,
						rf.role, maxTerm, rf.currentTerm)
				}()
				if rf.role != ROLE_CANDIDATES {
					return
				}
				// 任期没有peer大  自身将为follower
				if maxTerm > rf.currentTerm {
					rf.role = ROLE_FOLLOWER
					rf.leaderId = -1
					rf.currentTerm = maxTerm
					rf.votedFor = -1
					rf.persist()
					return
				}
				// 票数过半  当选leader
				if voteCount > len(rf.peers)/2 {
					rf.role = ROLE_LEADER
					rf.leaderId = rf.me
					rf.lastBroadcastTime = time.Unix(0, 0)
					return
				}
			}
		}()
	}
}

// #end region
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

// #region AppendEntries
type AppendEntriesArgs struct {
	Term         int      //领导人的任期
	LeaderId     int      //领导人 ID 因此跟随者可以对客户端进行重定向（译者注：跟随者根据领导人 ID 把客户端的请求重定向到领导人，比如有时客户端把请求发给了跟随者而不是领导人）
	PrevLogIndex int      //紧邻新日志条目之前的那个日志条目的索引
	PrevLogTerm  int      //紧邻新日志条目之前的那个日志条目的任期
	Entries      []string //需要被保存的日志条目（被当做心跳使用时，则日志条目内容为空；为了提高效率可能一次性发送多个）
	LeaderCommit int      //领导人的已知已提交的最高的日志条目的索引
}

type AppendEntriesReply struct {
	Term    int  //当前任期，对于领导人而言 它会更新自己的任期
	Success bool //如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了，则为 true
}

// 接收者是跟随者或者候选者  reply返回给leader
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("RaftNode[%d] Handle AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] role=[%s]",
		rf.me, args.LeaderId, args.Term, rf.currentTerm, rf.role)
	defer func() {
		DPrintf("RaftNode[%d] Return AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] role=[%s]",
			rf.me, args.LeaderId, args.Term, rf.currentTerm, rf.role)
	}()
	// if
	// 可能是不需要复制日志  因此直接返回false  仅把自己的term发回去即可
	reply.Term = rf.currentTerm
	reply.Success = false
	// leader的term小 直接返回失败即可 日志复制失败 leader会根据返回的Term对自己进行更新，降为follower
	if args.Term < rf.currentTerm {
		return
	}
	// 当前raft发现了新的leader 更新状态即可
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = ROLE_FOLLOWER
		rf.votedFor = -1
		rf.leaderId = -1
	}
	// 接收到心跳包  更新超时时间
	rf.leaderId = args.LeaderId
	rf.lastActiveTime = time.Now()
	rf.persist()
}

// 调用rpc 发送AppendEntries  调用远方的AppendEntries
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 心跳包和日志复制发送的信息
func (rf *Raft) appendEntriesLoop() {
	for !rf.killed() {
		time.Sleep(1 * time.Millisecond)
		// 仅leader可以广播发送appendEntries
		// 这样可以加个作用域  好加锁
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.role != ROLE_LEADER {
				return
			}
			now := time.Now()
			// 100ms广播一次
			if now.Sub(rf.lastBroadcastTime) < 100*time.Millisecond {
				return
			}
			// 更新最近广播时间
			rf.lastBroadcastTime = time.Now()
			// 广播所有的peer 发送信息
			for peerId := 0; peerId < len(rf.peers); peerId++ {
				if peerId == rf.me {
					continue
				}
				// 2A只是发送心跳包  没有日志信息的
				args := AppendEntriesArgs{
					Term:     rf.currentTerm,
					LeaderId: rf.me,
				}
				// 并发的发送信息接受响应
				go func(id int, args1 *AppendEntriesArgs) {
					DPrintf("RaftNode[%d] appendEntries starts, myTerm[%d] peerId[%d]", rf.me, args1.Term, id)
					reply := AppendEntriesReply{}
					if ok := rf.sendAppendEntries(id, args1, &reply); ok {
						//  如果返回的term大于自己，自己leader地位不保
						if reply.Term > rf.currentTerm {
							rf.role = ROLE_FOLLOWER
							rf.leaderId = -1
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.persist()
						}
						DPrintf("RaftNode[%d] appendEntries ends, peerTerm[%d] myCurrentTerm[%d] myRole[%s]", rf.me, reply.Term, rf.currentTerm, rf.role)
					}
				}(peerId, &args)
			}
		}()
	}
}

// #end region
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
	rf.readPersist(persister.ReadRaftState())
	// 监听是否需要选举，里面有定时器
	go rf.electionLoop()
	// 心跳包和日志信息不断从这里发送
	go rf.appendEntriesLoop()

	return rf
}
