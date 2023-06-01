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
	Term     int // Leader's term number
	LeaderID int // Leader's peer ID

	// PrevLogIndex int // Index of new log's first entry
	// PrevLogTerm  int // Term no. of PrevLogIndex entry
	// Entries []*LogEntry // New entries coming from leader (empty for heartbeat messages)
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

	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote

}

// example RequestVote RPC handler.

// RequestVote(candidateArgs, reply) -
//
//	candidateArgs:	informação do candidato emissor
//	reply:			resposta do eleitor, concedendo ou não o voto
//
//		Avalia se o candidato é válido para receber um novo voto originário do receptor,
//	escrevendo na resposta seu termo e se o voto foi concedido. Caso o candidato possua
//	um termo maior ou igual, receptor atualiza seu termo e reseta para estado follower caso necessário.
//	Caso não tenha votado ainda, estabelece-o como candidato. Caso as duas condições sejam atendidas,
//	garante seu voto ao candidato.
func (rf *Raft) RequestVote(candidateArgs *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()

	// rf - eleitor || args - candidate

	// Example logic:
	// 1. Check if the candidate's term is greater than the current term.
	newerTerm := false
	if candidateArgs.Term > rf.currentTerm {
		newerTerm = true
		rf.currentTerm = candidateArgs.Term
		rf.votedFor = -1
		rf.role = Follower
	}

	// 2. Determine if the candidate's log is up-to-date.
	// Não necessário para o teste 2A

	// 3. Check if the server has already voted for another candidate in this term.
	//    If it has, deny the vote.
	hasntVoted := rf.votedFor == -1

	// 4. Grant the vote to the candidate if all conditions are met.
	// Fill the reply struct with the appropriate values.
	if newerTerm && hasntVoted {
		reply.VoteGranted = true
		rf.votedFor = candidateArgs.CandidateID

	} else {
		reply.VoteGranted = false
	}

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

// Candidato requisita ao componente alvo que avalie a votação sobre ele. Impulsiona
// a chamada do método "RequestVote" pelo eleitor e espera pela resposta. Caso o eleitor aceite,
// incrementa a quantidade de eleitores que possui. Caso receba a maioria dos eleitores do sistema,
// altera estado para Leader e inicia a rotina de heartbeat para todos os componentes do sistema,
// finalizando então a eleição com sucesso.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	if ok && rf.role == Candidate {
		if reply.VoteGranted {
			rf.votesReceived += 1

			if rf.isAlive {
				fmt.Printf("Server %d received a vote from %d \n", rf.me, server)
				fmt.Printf("Server %d now has %d votes out of %d \n", rf.me, rf.votesReceived, len(rf.peers))
			}
		}

		if rf.votesReceived > (len(rf.peers) / 2) {
			if rf.isAlive {
				fmt.Printf("Server %d is has been elected leader of Term %d\n", rf.me, rf.currentTerm)
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

// AppendEntry(args, reply) -
//
//	args:	informações do líder emissor
//	reply:	resposta fornecida pelo componente receptor, validando o emissor como líder ou não
//
//		Comunicação realizada entre o líder e os componentes do sistema. Aciona a leitura
//	de heartbeat do componente receptor e avalia se o líder emissor ainda é válido para a conjuntura
//	do sistema, informando-o se deve retornar ou não ao estado de Follower. Além disso, avalia a
//	validade do próprio receptor, retornando-o ao estado de Follower caso seja o necessário.
func (rf *Raft) AppendEntry(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.heartbeatCh <- args

	rf.mu.Lock()

	reply.Success = true

	if args.Term >= rf.currentTerm {
		if (rf.isAlive) { fmt.Printf("Server [S%d | T%d] stepped down in place of [S%d | T%d] and updated its Term\n", rf.me, rf.currentTerm, args.LeaderID, args.Term)
		rf.role = Follower
		rf.currentTerm = args.Term
	}

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
	}

	if reply.Success {
		if rf.isAlive {
			fmt.Printf("	Message Received: Server [S%d | T%d] <--- Leader [S%d | T%d] \n", rf.me, rf.currentTerm, args.LeaderID, args.Term)
		}
	} else {
		if rf.isAlive {
			fmt.Printf("	Server %d has sent a failed message. (Outdated Leader) \n", args.LeaderID)
		}
	}

	rf.mu.Unlock()

}

// startHeartbeatRoutine() -
//
//		Inicializa a rotina de heartbeat de um líder quando eleito. Permanece em execução
//	enquanto o componente for líder, informando os outros componentes de sua atividade e
//	evitando que novas eleições sejam inicializadas. Impulsiona a chamada do método
//	"AppendyEntry" pelo componente alvo a apartir de uma comunicação RPC, avaliando sua validade como líder.
func (rf *Raft) startHearbeatRoutine() {
	for {
		if rf.role != Leader || !rf.isAlive {

			return
		}

		hbArgs := AppendEntriesArgs{}
		hbArgs.Term = rf.currentTerm
		hbArgs.LeaderID = rf.me

		// Debug utilizado para verificar se corrotina se propagava entre testes.
		// fmt.Printf("Server %d [PID %f]\n", rf.me, rf.debugId)

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

						rf.mu.Lock()

						// Atualizando termo na resposta negativa e saindo de lider
						if rf.isAlive {
							fmt.Printf("Server %d: Message received with failed response, stepping down from leadership", rf.me)
						}
						rf.role = Follower
						rf.currentTerm = hbReply.Term

						rf.mu.Unlock()
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
	// Desativando server ao fim do teste
	rf.isAlive = false
}

// startElection() -
//
//		Inicializa o processo de eleição. O componente requisitor se torna candidato, enviando
//	uma requisição para todos os componentes do sistema e informando suas características, necessárias
//	para a aceitação ou recusa do eleitor. Um mutex lock é implementado para evitar discordância de acesso
//	durante o processamento de múltiplos candidatos.
func (rf *Raft) startElection() {

	rf.mu.Lock()

	fmt.Printf("Starting new election: [S%d | T%d] is trying to become a leader of term %d. \n", rf.me, rf.currentTerm, rf.currentTerm+1)

	rf.role = Candidate
	rf.currentTerm = rf.currentTerm + 1 	//Current term increments on start of election
	rf.votedFor = rf.me
	rf.votesReceived = 1

	requestVotesArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.lastCommitedLogIndex,
		LastLogTerm:  rf.lastAppliedLogIndex,
	}

	//Broadcast vote request message to all servers
	for id := 0; id < len(rf.peers); id++ {
		if id == rf.me {
			//Skip itself
			continue
		}
		fmt.Printf("Candidate [S%d | T%d] is aking %d's vote.\n", rf.me, rf.currentTerm, id)

		requestVotesReply := RequestVoteReply{}
		go rf.sendRequestVote(id, &requestVotesArgs, &requestVotesReply)
	}

	rf.mu.Unlock()
}

// listenHeartBeat() -
//
//	Gerencia de forma assíncrona a escuta de heartbeat de todos os componentes do sistema,
//	implementando comportamento específico para os candidatos. Caso o contador de um follower
//	ou Candidate esgote, sinaliza o início de uma eleição com o seu termo incrementado e suas variaveis resetadas.
func (rf *Raft) listenHearbeat() {

	timeoutTimer := time.NewTimer(rf.timeoutDuration)

	for {
		if !rf.isAlive {
			return
		}
		select {
		case hb := <-rf.heartbeatCh:

			rf.mu.Lock()

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

			rf.mu.Unlock()
		case <-timeoutTimer.C:

			if rf.role != Leader { // Election timeout elapsed, trigger election
				fmt.Printf("Follower %d has timed out during term %d. \n", rf.me, rf.currentTerm)

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

// Make() -
//
//		Inicia um novo compoenente do sistema em estado de Follower, especificando os
//	valores iniciais para cada variável necessitada pela implementação do algoritmo.
//	Inicia a contagem de election timeout de cada um desses componentes com a função
//	listenHeartbeat(), aplicando um valor aleatório entre 250ms e 400ms, de acordo
//	com o enunciado do trabalho.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf.me = me
	println(">	Initializing new raft server %d	<\n", rf.me)

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister

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
