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
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

type Role int

const (
	Leader Role = iota
	Follower
	Candidate
)

// AppendEntry

type AppendEntriesArgs struct {
	Term int // Leader's term number

	//* Variaveis inuteis se não for trabalhar com logs (Não necessário para o 2A aparentemente)
	LeaderID int // Leader's peer ID
	// PrevLogIndex int // Index of new log's first entry
	// PrevLogTerm  int // Term no. of PrevLogIndex entry
	// // Entries []*LogEntry // New entries coming from leader (empty for heartbeat messages)
	// LeaderCommit int // Leader's committ index
}

type AppendEntriesReply struct {
	Term    int  // Peer's term number for leader to update itself
	Success bool // True if peer successfully stored new entries
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	isAlive     bool                // updated by kill
	leaderIndex int                 // current leader's index
	role        Role
	listener    int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	currentTerm   int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor      int        // index of peer voted, null if not voted
	logArray      []stateLog // log entries; each entry contains command	for state machine, and term when entry was received by leader (first index is 1)
	votesReceived int

	// volatile state on all servers
	lastCommitedLogIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastAppliedLogIndex  int // index of highest log entry applied to state machine (initialized to 0, increases	monotonically)

	//max and min time for random duration
	maxTime         int
	minTime         int
	timeoutDuration time.Duration
	heartbeatCh     chan *AppendEntriesArgs

	debugId float64
}

type stateLog struct {
	Log      string
	LogIndex int
	LogTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	return rf.currentTerm, rf.role == Leader

}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // Candidate's current term
	CandidateID  int // ID of the candidate requesting the vote
	LastLogIndex int // Index of the candidate's last log entry
	LastLogTerm  int // Term of the candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).

	Term        int  //TODO currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(candidateArgs *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()

	// rf - eleitor || args - candidate

	// Example logic:
	// 1. Check if the candidate's term is greater than the current term.
	newerTerm := false
	if candidateArgs.Term >= rf.currentTerm {
		newerTerm = true
		rf.currentTerm = candidateArgs.Term
		rf.votedFor = -1
		rf.role = Follower
	}

	// 2. Determine if the candidate's log is up-to-date.
	//    Compare the last log term and index with the local log.
	// upToDate := false
	// if args.LastLogTerm > rf.logArray[len(rf.logArray)-1].LogTerm || (args.LastLogTerm == rf.logArray[len(rf.logArray)-1].LogTerm && args.LastLogIndex >= rf.logArray[len(rf.logArray)-1].LogIndex) {
	// 	upToDate = true
	// }

	// 3. Check if the server has already voted for another candidate in this term.
	//    If it has, deny the vote.
	hasntVoted := rf.votedFor == -1
	// 4. Grant the vote to the candidate if all conditions are met.

	if newerTerm && hasntVoted {
		reply.VoteGranted = true
		rf.votedFor = candidateArgs.CandidateID
		// rf.resetElectionTimer()
	} else {
		reply.VoteGranted = false
	}

	// Fill the reply struct with the appropriate values.
	reply.Term = rf.currentTerm

	rf.mu.Unlock()
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

	rf.mu.Lock()
	if ok && rf.role == Candidate {
		if reply.VoteGranted {
			rf.votesReceived += 1
		}

		if rf.votesReceived > (len(rf.peers) / 2) {
			if rf.isAlive {
				fmt.Printf("Server %d got a vote from %d \n", rf.me, server)
			}
			if rf.isAlive {
				fmt.Printf("Server %d he now has %d votes out of 3 \n", rf.me, rf.votesReceived)
			}
			if rf.isAlive {
				fmt.Printf("Server %d IS NOW A LEADER. ALL YOUR BASES ARE BELONG TO US | Term %d \n", rf.me, rf.currentTerm)
			}
			rf.role = Leader
			rf.votedFor = -1
			rf.votesReceived = 0
			rf.leaderIndex = rf.me

			go rf.startHearbeatRoutine()
		}
	}
	rf.mu.Unlock()
	return ok
}

func (rf *Raft) AppendEntry(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.heartbeatCh <- args

	rf.mu.Lock()

	if rf.isAlive {
		fmt.Printf("	Server [%d/T%d] <-- Leader [%d/T%d] \n", rf.me, rf.currentTerm, args.LeaderID, args.Term)
	}
	reply.Success = true

	if args.Term >= rf.currentTerm {
		rf.role = Follower
		rf.currentTerm = args.Term
		// if (rf.isAlive) { fmt.Printf("Server %d [%d] should have stepped down in place of %d [%d]. \n", rf.me, rf.currentTerm, args.LeaderID, args.Term)
	}

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
	}

	if reply.Success {
		if rf.isAlive {
			fmt.Printf("		Server %d has received a message from %d. \n", rf.me, args.LeaderID)
		}
	} else {
		if rf.isAlive {
			fmt.Printf("%d has sent a failed message. \n", args.LeaderID)
		}
	}

	rf.mu.Unlock()

}

func (rf *Raft) startHearbeatRoutine() {
	for {
		if rf.role != Leader || !rf.isAlive {

			return
		}

		hbArgs := AppendEntriesArgs{}
		hbArgs.Term = rf.currentTerm
		hbArgs.LeaderID = rf.me

		fmt.Printf("PID %f\n", rf.debugId)

		//Broadcast heartbeat para todos os servidores
		for id := 0; id < len(rf.peers); id++ {

			if id == rf.me {

				continue
			}

			hbReply := AppendEntriesReply{}

			go func(id int) {
				ok := rf.peers[id].Call("Raft.AppendEntry", &hbArgs, &hbReply)

				if ok {

					if !hbReply.Success {

						rf.role = Follower

					}
				}
			}(id)
		}

		time.Sleep(100 * time.Millisecond)
	}

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.isAlive = false
}

func (rf *Raft) startElection() {

	rf.mu.Lock()

	if rf.isAlive {
		fmt.Printf("Server %d current term before election %d. \n", rf.me, rf.currentTerm)
	}

	rf.role = Candidate
	//Current term increments on start of election
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.me
	rf.votesReceived = 1

	if rf.isAlive {
		fmt.Printf("Follower %d is trying to become a candidate of term %d. \n", rf.me, rf.currentTerm)
	}

	requestVotesArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.lastCommitedLogIndex,
		LastLogTerm:  rf.lastAppliedLogIndex,
	}

	//Broadcast vote request message to all servers
	for id := 0; id < len(rf.peers); id++ {
		if id == rf.me {
			continue
		}
		if rf.isAlive {
			fmt.Printf("Follower %d is trying to ask vote for %d. \n", rf.me, id)
		}
		requestVotesReply := RequestVoteReply{}

		go rf.sendRequestVote(id, &requestVotesArgs, &requestVotesReply)
	}

	rf.mu.Unlock()
}

func (rf *Raft) listenHearbeat() {

	timeoutTimer := time.NewTimer(rf.timeoutDuration)

	for {
		if !rf.isAlive {
			return
		}
		select {
		case hb := <-rf.heartbeatCh:

			if rf.role == Candidate {
				if rf.isAlive {
					fmt.Printf("Follower %d has stepped down from candidature. \n", rf.me)
				}

				rf.role = Follower
				rf.votedFor = -1
				rf.votesReceived = 0
				rf.leaderIndex = hb.LeaderID
			}
			// Received heartbeat
			// Reset election timeout
			timeoutTimer.Reset(rf.timeoutDuration)
			rf.timeoutDuration = time.Duration(rand.Intn(rf.maxTime-rf.minTime+1)+rf.minTime) * time.Millisecond

		case <-timeoutTimer.C:

			if rf.role != Leader { // Election timeout elapsed, trigger election
				if rf.isAlive {
					fmt.Printf("Follower %d has timeout'ed during term %d, send help. \n", rf.me, rf.currentTerm)
				}
				// server int, args *RequestVoteArgs, reply *RequestVoteReply

				rf.startElection()
				timeoutTimer.Reset(rf.timeoutDuration)
				rf.timeoutDuration = time.Duration(rand.Intn(rf.maxTime-rf.minTime+1)+rf.minTime) * time.Millisecond

			}
		}
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

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.isAlive = true
	rf.debugId = rand.Float64() * 100

	// Your initialization code here (2A, 2B, 2C).
	rf.leaderIndex = -1

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.votesReceived = 0
	rf.lastCommitedLogIndex = -1
	rf.lastAppliedLogIndex = -1

	rf.minTime = 250
	rf.maxTime = 400
	rf.timeoutDuration = time.Duration(rand.Intn(rf.maxTime-rf.minTime+1)+rf.minTime) * time.Millisecond

	rf.role = Follower

	rf.heartbeatCh = make(chan *AppendEntriesArgs)

	go rf.listenHearbeat()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
