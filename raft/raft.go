package raft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new Log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the Log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

// as each Raft peer becomes aware that successive Log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed Log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type state int

const (
	follower state = iota
	candidate
	leader
)
const (
	HeartbeatTimeout = 800
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu              sync.Mutex          // Lock to protect shared access to this peer's state
	peers           []*labrpc.ClientEnd // RPC end points of all peers
	persister       *Persister          // Object to hold this peer's persisted state
	me              int                 // this peer's index into peers[]
	dead            int32               // set by Kill()
	state           state
	heartbeat       time.Time
	ApplyMsgchannel chan ApplyMsg
	//Persistent state on all servers:
	CurrenTerm int        //latest term server has seen (initialized to 0 on first boot, increases monotonically)
	VotedFor   int        //candidateId that received vote in current term (or null if none)
	Log        []Logentry //Log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	//Volatile state on all servers:
	commitIndex int //index of highest Log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int //index of highest Log entry applied to state machine (initialized to 0, increases monotonically)
	//Volatile state on leaders:
	nextIndex  []int //for each server, index of the next Log entry to send to that server (initialized to leader last Log index + 1)
	matchIndex []int //for each server, index of highest Log entry known to be replicated on server (initialized to 0, increases monotonically)
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type Logentry struct {
	Command interface{}
	Term    int
}

type AppendEntriesArgs struct {
	Term         int        //  leader’s term
	LeaderId     int        //  so follower can redirect clients
	PreLogIndex  int        //  index of Log entry immediately preceding new ones
	PreLogTerm   int        //  term of prevLogIndex entry
	Entries      []Logentry //  Log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        //  leader’s commitIndex
}

type AppendEntriesReply struct {
	Term          int  //CurrenTerm, for leader to update itself
	Success       bool //true if follower contained entry matching prevLogIndex and prevLogTerm
	Conflictindex int
	Conflictterm  int
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//c2.mu.Lock()
	//c2.cv++
	//fmt.Println("c2", c2.cv)
	//c2.mu.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) updatecommitindex(newcommit int) {

	if newcommit > rf.commitIndex {
		rf.commitIndex = newcommit
	}
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Conflictindex = -1
	reply.Conflictterm = -1
	reply.Term = rf.CurrenTerm
	//DPrintf("from %v %v I %v %v", args.LeaderId, args.Term, rf.me, rf.CurrenTerm)
	//DPrintf("%v log %v", rf.me, rf.Log)
	if args.Term < rf.CurrenTerm {
		//fmt.Println(rf.me, "<")
		reply.Success = false
		return
	}
	if args.Term > rf.CurrenTerm {
		rf.CurrenTerm = args.Term
		rf.persist()
	}
	rf.changestate(follower)
	rf.resettime()
	if rf.getlastlogindex() < args.PreLogIndex {
		reply.Success = false
		reply.Conflictindex = rf.getlastlogindex() + 1
		//DPrintf("%v", reply.Conflictindex)
		return
	}
	if len(rf.Log) > args.PreLogIndex {
		if rf.Log[args.PreLogIndex].Term != args.PreLogTerm {
			reply.Conflictterm = rf.Log[args.PreLogIndex].Term
			reply.Success = false
			for i := args.PreLogIndex; i >= 0; i-- {
				if rf.Log[i].Term != rf.Log[args.PreLogIndex].Term {
					reply.Conflictindex = i + 1
					break
				}
			}
			return
		}
	}

	reply.Success = true

	rf.Log = append(rf.Log[:args.PreLogIndex+1], args.Entries...)
	rf.persist()
	//var p = len(rf.Log) - args.PreLogIndex
	//for i := 0; i < min(len(args.Entries), len(rf.Log)-args.PreLogIndex); i++ {
	//	if rf.Log[i+args.PreLogIndex] != args.Entries[i] {
	//		rf.Log = rf.Log[:i+args.PreLogIndex]
	//		p = i
	//		break
	//	}
	//}
	//for i := p; i < len(args.Entries); i++ {
	//	rf.Log = append(rf.Log, args.Entries[i])
	//}
	var newcommit = min(args.LeaderCommit, rf.getlastlogindex())
	rf.updatecommitindex(newcommit)

}

// return CurrenTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	term, isleader = rf.CurrenTerm, rf.state == leader
	// Your code here (2A).
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrenTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logg []Logentry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logg) != nil {
		log.Fatalf("Decode error")
	} else {
		rf.CurrenTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Log = logg
	}

}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the Log through (and including)
// that index. Raft should now trim its Log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //candidate’s term
	CandidateId  int //candidate requesting vote
	LastLogIndex int //index of candidate’s last Log entry
	LastLogTerm  int //term of candidate’s last Log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  //CurrenTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
	// Your data here (2A).
}

func (rf *Raft) checkupdate(lastlogindex int, lastlogterm int) bool {
	return rf.Log[rf.getlastlogindex()].Term < lastlogterm || rf.Log[rf.getlastlogindex()].Term == lastlogterm && rf.getlastlogindex() <= lastlogindex
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.CurrenTerm {
		reply.VoteGranted = false
		reply.Term = rf.CurrenTerm
		return
	}
	if args.Term > rf.CurrenTerm {
		rf.changestate(follower)
		rf.CurrenTerm = args.Term
		rf.VotedFor = -1
		rf.persist()
	}
	reply.Term = rf.CurrenTerm

	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) && rf.checkupdate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateId
		rf.persist()
		rf.resettime()
	}
	// Your code here (2A, 2B).
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
	//c1.mu.Lock()
	//c1.cv++
	//c1.mu.Unlock()

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's Log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft Log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()

	index := rf.getlastlogindex() + 1
	term := rf.CurrenTerm
	isLeader := rf.state == leader

	if isLeader {
		rf.Log = append(rf.Log, Logentry{
			Command: command,
			Term:    term,
		})
		rf.persist()
		rf.nextIndex[rf.me] = rf.getlastlogindex() + 1
		rf.matchIndex[rf.me] = rf.getlastlogindex()
	}
	rf.mu.Unlock()
	// Your code here (2B).
	return index, term, isLeader
}

func (rf *Raft) leadercommit() {
	for rf.killed() == false {
		time.Sleep(50 * time.Millisecond)
		rf.mu.Lock()
		if rf.state != leader {
			rf.mu.Unlock()
			return
		}
		cnt := 0
		for n := rf.commitIndex + 1; n <= rf.getlastlogindex(); n++ {
			cnt = 0
			for i := range rf.peers {
				if rf.matchIndex[i] >= n {
					cnt++
				}
			}
			if cnt > len(rf.peers)/2 && rf.Log[n].Term == rf.CurrenTerm {
				rf.commitIndex = n
				break
			}
		}

		rf.mu.Unlock()
	}
}

//func (rf *Raft) commit(msg ApplyMsg) {
//	rf.ApplyMsgchannel <- msg
//}

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

func (rf *Raft) resettime() {
	PERIOD_TIME := rand.New(rand.NewSource(time.Now().UnixNano())).Int31()%400 + 800
	rf.heartbeat = time.Now()
	rf.heartbeat = rf.heartbeat.Add(time.Duration(PERIOD_TIME) * time.Millisecond)
}

func (rf *Raft) leaderappend() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {

			for rf.killed() == false {
				rf.mu.Lock()
				if rf.state != leader {
					rf.mu.Unlock()
					break
				}
				args := &AppendEntriesArgs{
					Term:         rf.CurrenTerm,
					LeaderId:     rf.me,
					PreLogIndex:  rf.nextIndex[server] - 1,
					PreLogTerm:   rf.Log[min(rf.getlastlogindex(), rf.nextIndex[server]-1)].Term,
					LeaderCommit: rf.commitIndex,
				}
				if rf.nextIndex[server] <= rf.getlastlogindex() {
					args.Entries = rf.Log[rf.nextIndex[server]:]
				} else {
					args.Entries = nil
				}
				rf.mu.Unlock()
				go func() {
					var ok bool
					for rf.killed() == false {
						rf.mu.Lock()
						if rf.state != leader {
							rf.mu.Unlock()
							return
						}
						rf.mu.Unlock()
						var reply = &AppendEntriesReply{}
						//DPrintf("%v try to %v", rf.me, server)
						ok = rf.sendAppendEntries(server, args, reply)
						if ok == false {
							//DPrintf("%v to %v disconn", rf.me, server)
							break
						}
						//DPrintf("%v to %v", rf.me, server)
						rf.mu.Lock()
						if reply.Term > rf.CurrenTerm {
							rf.CurrenTerm = reply.Term
							rf.persist()
							rf.changestate(follower)
							//DPrintf("%v to follower", rf.me)
							rf.resettime()
							rf.mu.Unlock()
							return
						}
						if reply.Success == true {
							rf.matchIndex[server] = args.PreLogIndex + len(args.Entries)
							rf.nextIndex[server] = rf.matchIndex[server] + 1
							rf.mu.Unlock()
							break
						} else if reply.Conflictindex != -1 {
							if reply.Term == -1 {
								rf.nextIndex[server] = reply.Conflictindex
							}
							for j := min(rf.nextIndex[server]-1, rf.getlastlogindex()+1); j >= reply.Conflictindex; j-- {
								if rf.Log[j-1].Term == reply.Conflictterm || j == reply.Conflictindex {
									rf.nextIndex[server] = j
									break
								}
							}
							rf.nextIndex[server] = min(rf.nextIndex[server], rf.getlastlogindex()+1)
							args.PreLogIndex = rf.nextIndex[server] - 1
							args.PreLogTerm = rf.Log[rf.nextIndex[server]-1].Term
							args.Entries = rf.Log[rf.nextIndex[server]:]
							rf.mu.Unlock()

						} else {
							rf.mu.Unlock()
							break
						}
					}
				}()
				time.Sleep(110 * time.Millisecond)
			}

		}(i)
	}
}
func (rf *Raft) apply() {
	for rf.killed() == false {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()

		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			msg := ApplyMsg{
				CommandValid:  true,
				Command:       rf.Log[rf.lastApplied].Command,
				CommandIndex:  rf.lastApplied,
				SnapshotValid: false,
				Snapshot:      nil,
				SnapshotTerm:  0,
				SnapshotIndex: 0,
			}
			rf.ApplyMsgchannel <- msg
		}
		rf.mu.Unlock()
	}
}
func (rf *Raft) changestate(state state) {
	if state == leader {
		//DPrintf("leader %v", rf.me)
		for i := range rf.peers {
			rf.nextIndex[i] = rf.getlastlogindex() + 1
			if i != rf.me {
				rf.matchIndex[i] = 0
			} else {
				rf.matchIndex[i] = rf.getlastlogindex()
			}
		}
		go rf.leadercommit()
		go rf.leaderappend()
	}
	rf.state = state
}
func (rf *Raft) getlastlogindex() int {
	return len(rf.Log) - 1
}
func (rf *Raft) getlastlogterm() int {
	return rf.Log[rf.getlastlogindex()].Term
}
func (rf *Raft) startelection() {
	rf.resettime()
	//DPrintf("elect %v", rf.me)
	rf.CurrenTerm++
	rf.VotedFor = rf.me
	rf.persist()
	cnt := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			args := &RequestVoteArgs{
				Term:         rf.CurrenTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.getlastlogindex(),
				LastLogTerm:  rf.getlastlogterm(),
			}
			if rf.state != candidate {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			reply := &RequestVoteReply{
				Term:        0,
				VoteGranted: false,
			}

			ok := rf.sendRequestVote(server, args, reply)
			if ok == true {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.state != candidate {
					return
				}

				if reply.VoteGranted == true {
					cnt++
					if cnt > len(rf.peers)/2 && rf.state == candidate {
						rf.changestate(leader)
						return
					}
				} else if reply.Term > rf.CurrenTerm {
					rf.changestate(follower)
					rf.CurrenTerm = reply.Term
					rf.resettime()
					rf.VotedFor = -1
					rf.persist()
					return
				}
			}
		}(i)

	}

}

//func (rf *Raft) sendHeartbeat() {
//	for i, _ := range rf.peers {
//		if i == rf.me {
//			continue
//		}
//		//if rf.killed() {
//		//	return
//		//}
//		go func(server int) {
//			rf.mu.Lock()
//			args := &AppendEntriesArgs{
//				Term:         rf.CurrenTerm,
//				LeaderId:     rf.me,
//				PreLogIndex:  rf.n,
//				PreLogTerm:   0,
//				Entries:      nil,
//				LeaderCommit: rf.commitIndex,
//			}
//			reply := &AppendEntriesReply{
//				Term:    -1,
//				Success: false,
//			}
//			rf.mu.Unlock()
//			ok := rf.sendAppendEntries(server, args, reply)
//			if ok == true {
//				rf.mu.Lock()
//				defer rf.mu.Unlock()
//				if reply.Term > rf.CurrenTerm {
//					rf.changestate(follower)
//					rf.VotedFor = -1
//					return
//				}
//			}
//
//		}(i)
//	}
//
//}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	for rf.killed() == false {
		time.Sleep(10 * time.Millisecond)
		//time.Sleep(time.Duration(PERIOD_TIME) * time.Millisecond)
		rf.mu.Lock()
		switch rf.state {
		case follower:
			if time.Now().Sub(rf.heartbeat) > time.Millisecond*HeartbeatTimeout {
				rf.changestate(candidate)
				rf.startelection()
			}
		case candidate:
			if time.Now().Sub(rf.heartbeat) > time.Millisecond*HeartbeatTimeout {
				rf.startelection()
			}
		}
		rf.mu.Unlock()
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

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

	rf := &Raft{
		mu:              sync.Mutex{},
		peers:           peers,
		persister:       persister,
		me:              me,
		dead:            0,
		state:           follower,
		heartbeat:       time.Now(),
		ApplyMsgchannel: applyCh,
		CurrenTerm:      0,
		VotedFor:        -1,
		Log:             make([]Logentry, 0, 100),
		commitIndex:     0,
		lastApplied:     0,
		nextIndex:       make([]int, len(peers), 100),
		matchIndex:      make([]int, len(peers), 100),
	}
	rf.Log = append(rf.Log, Logentry{
		Command: nil,
		Term:    -1,
	})
	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.resettime()
	go rf.apply()
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
