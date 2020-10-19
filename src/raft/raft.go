package raft

import (
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// 具体发送命令的内容 不涉及本lab
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

}

//
// raft实体的struct结构
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// raft的状态
	state RaftState

	// 目前的term
	currentTerm int

	// 这轮投给了谁，如果-1代表没有投票
	votedFor int

	// 日志集合
	log *LogEntries

	// leader的ch 如果有follower的term大于leader的term 会将内容放入leaderCh
	leaderCh chan *AppendEntryReply

	// 每次心跳有效 都会在messageCh中放入true来重置响应时间
	messageCh chan bool
}
// raft的三种state
type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

type LogEntries struct {
	Empty bool
	Entries []*Entry
}

// 为空代表发送心跳 不为空代表发送日志
// 在本实验中 心跳的rpc中的args和reply的结构
type AppendEntryArgs struct {
	Term     int
	LeaderId int
	Entries  *LogEntries

}

type AppendEntryReply struct {
	Term    int
	Success bool
}

type Entry struct {
	Command interface{}
	Term int
}

// start负责从上层接收命令 写入到leader的日志中 不涉及本lab
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	return -1, -1, true
}

// 接收AppendEntries的具体逻辑
// 在本lab中 你需要实现心跳的具体处理逻辑
// args为发送的参数
// reply为回复
func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	// in this lab, you should do this...

}

// 发送AppendEntries的RPC调用
func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 返回state
func (rf *Raft) GetRaftState() RaftState {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

// 得到raft的状态
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	term = rf.getCurrentTerm()
	isleader = rf.GetRaftState() == Leader

	return term, isleader
}

// 在本实验中 投票的rpc中的args和reply的结构
type RequestVoteArgs struct {
	Term int
	CandidateId int
}

type RequestVoteReply struct {
	Term int
	VoteGranted bool
}

// 接收RequestVote的具体逻辑
// args为发送的参数
// reply为回复
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	term := rf.getCurrentTerm()
	if args.Term < term {
		reply.Term = term
		reply.VoteGranted = false
		DPrintf("%d(term %d) deny %d(term %d) because of term", rf.me, rf.getCurrentTerm(), args.CandidateId, args.Term)
	} else {
		if args.Term > term {
			rf.setCurrentTerm(args.Term)
			rf.setState(Follower)
		}
		if rf.getVotedFor() != -1{
			reply.VoteGranted = false
			reply.Term = args.Term
			DPrintf("%d(term %d) deny %d(term %d) because of leader", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		} else {
			reply.VoteGranted = true
			reply.Term = args.Term
			rf.setVotedFor(args.CandidateId)
			rf.setState(Follower)
			rf.setCurrentTerm(args.Term)
			DPrintf("%d(term %d) vote for %d(term %d)", rf.me, rf.getCurrentTerm(), args.CandidateId, args.Term)
		}
	}

}

// 发送RequestVote的RPC调用
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// kill可以杀掉具体raft，不涉及本lab
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

// killed代表raft是否已经被kill掉，不涉及本lab
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 有一个线程用来根据raft的不同状态调用不同的函数
func (rf *Raft) run() {
	for {
		switch rf.GetRaftState() {
		case Follower:
			rf.runFollower()
		case Candidate:
			rf.runCandidate()
		case Leader:
			rf.runLeader()
		}
	}
}

// 如果followerTimer的到期 就变成candidate开始进行选举
// 如果接收到心跳 重置timer
func (rf *Raft) runFollower() {
	followerTimer := randomTimeout()
	for rf.GetRaftState() == Follower {
		select {
		case <-followerTimer:
			rf.setState(Candidate)
			break
		case <-rf.messageCh:
			followerTimer = randomTimeout()
		}
	}
}

// 开始进行选举 并且生成一个electionTimer
// 如果electionTimer到期或者接收到来自于其他主机的心跳 代表已经有leader了
// 就立即返回
func (rf *Raft) runCandidate() {
	electionTimer := randomTimeout()
	rf.setCurrentTerm(rf.getCurrentTerm() + 1)
	// DPrintf("%d(term %d) start to election", rf.me, rf.currentTerm)


	electionCh := rf.electSelf(rf.getCurrentTerm())
	voteNeeded := len(rf.peers) / 2 + 1
	voteNow := 0


	for rf.GetRaftState() == Candidate {
		electionTimer = randomTimeout()
		select {
		case vote := <-electionCh:
			//正在进行选举
			if vote.Term > rf.getCurrentTerm() {
				rf.setCurrentTerm(vote.Term)
				rf.setState(Follower)
				return
			}
			if vote.Term == rf.getCurrentTerm() && vote.VoteGranted == true {
				voteNow += 1
				DPrintf("%d(term %d) get a vote, vote Now is %d ", rf.me, rf.currentTerm, voteNow)
			}
			if voteNow >= voteNeeded {
				rf.setState(Leader)
				return
			}
		case <-electionTimer:
			//很久没有接收到心跳 重新开始
			// DPrintf("%d(term %d) don't get heartbeat(message)", rf.me, rf.currentTerm)
			return
		case <-rf.messageCh:
			//收到心跳
			// DPrintf("%d(term %d) get a heartbeat(message)", rf.me, rf.currentTerm)
			rf.setState(Follower)
			return
		}
	}
}

// leader首先发送一轮心跳
// 之后如果接收到来自于term更高的心跳的返回信息(reply) 就变成follower
func (rf *Raft) runLeader() {


	term := rf.getCurrentTerm()

	go rf.sendHeartBeat(term)


	for rf.GetRaftState() == Leader {
		heartBeatTimer := heartBeatTimeout()
		select {
		case <-heartBeatTimer:
			go rf.sendHeartBeat(rf.getCurrentTerm())

		case reply := <-rf.leaderCh:
			if reply.Term > rf.getCurrentTerm() {
				rf.setCurrentTerm(reply.Term)
				rf.setState(Follower)
				return
			}
		}
	}
}

// 向每个主机发送一轮心跳
func (rf *Raft) sendHeartBeat(term int) {
	aeArgs := &AppendEntryArgs{
		Term:         term,
		LeaderId:     rf.me,
		Entries:      &LogEntries{Empty: true},
	}
	for i := 0; i < len(rf.peers); i ++ {
		if i != rf.me {
			go func(term int, server int) {
				// 让已经过时的channel不在发送心跳
				currentTerm,isLeader := rf.GetState()
				if currentTerm != term || isLeader == false {
					return
				}
				// DPrintf("%d(term %d) start to send heartbeat to %d", rf.me, term, server)
				aeReply := &AppendEntryReply{}
				rf.sendAppendEntries(server, aeArgs, aeReply)
				if aeReply.Success == false {
					rf.leaderCh <-aeReply
				}
			}(term, i)
		}
	}
}

// 设置raft的状态
func (rf *Raft) setState(state RaftState) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if state == Follower {
		DPrintf("%d(term %d) become follower", rf.me, rf.currentTerm)
	} else if state == Candidate {
		DPrintf("%d(term %d) become candidate", rf.me, rf.currentTerm)
	} else {
		DPrintf("%d(term %d) become leader", rf.me, rf.currentTerm)
	}
	rf.state = state
	return
}

// 设置term 并且term不会减小
func (rf *Raft) setCurrentTerm(term int) int {
	rf.mu.Lock()
	// DPrintf("%d(term %d) become %d(term %d)", rf.me, rf.currentTerm, rf.me, term)
	preTerm := rf.currentTerm
	if rf.currentTerm < term {
		rf.currentTerm = term
		rf.votedFor = -1
	}
	rf.mu.Unlock()
	return preTerm
}

// 获得term
func (rf *Raft) getCurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

// 设置voteFor
func (rf *Raft) setVotedFor(votedFor int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	preVotedFor := rf.votedFor
	rf.votedFor = votedFor
	return preVotedFor
}

// 得到本轮的voteFor
func (rf *Raft) getVotedFor() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.votedFor
}

// 生成一个随机时间的选举的channel，一段时间之后，生成一个时间放入这个channel
func randomTimeout() <-chan time.Time {
	return time.After(time.Duration(rand.Intn(150) + 150) * time.Millisecond)
}

// 生成一个随机时间的心跳的channel，一段时间之后，生成一个时间放入这个channel
func heartBeatTimeout() <-chan time.Time {
	return time.After(time.Duration(50) * time.Millisecond)
}

// 先选自己 然后发送一定的消息给其他的peer
func (rf *Raft) electSelf(term int) <-chan *RequestVoteReply {
	rvCh := make(chan *RequestVoteReply, len(rf.peers))
	req := &RequestVoteArgs{
		Term: term,
		CandidateId: rf.me,
	}
	askPeer := func(peer int) {
		go func() {
			reply := &RequestVoteReply{}
			// DPrintf("%d(term %d) send a request vote to %d", rf.me, term, peer)
			rf.sendRequestVote(peer, req, reply)
			rvCh<-reply
		}()
	}

	for i := 0; i < len(rf.peers); i ++ {
		if i == rf.me {
			// DPrintf("%d(term %d) vote for itself", rf.me, rf.getCurrentTerm())
			rf.setVotedFor(i)
			reply := &RequestVoteReply{}
			reply.Term = term
			reply.VoteGranted = true
			rvCh<-reply
		} else {
			askPeer(i)
		}
	}
	return rvCh
}

// 对raft这个结构进行初始化的工作
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.messageCh = make(chan bool)
	rf.leaderCh = make(chan *AppendEntryReply)

	rf.log = &LogEntries{
		Empty: true,
		Entries: make([]*Entry, 0),
	}

	go rf.run()

	return rf
}
