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

import "sync"
import "labrpc"
import "time"
import "errors"

// import "bytes"
// import "labgob"

type ServerState int

const  (
	Candidate ServerState = iota
	Follower
	Leader
	Stopped
	Initialized

)

const (
	MaxLogEntriesPerRequest         = 2000
	NumberOfLogEntriesAfterSnapshot = 200
)

const (
	// DefaultHeartbeatInterval is the interval that the leader will send
	// AppendEntriesRequests to followers to maintain leadership.
	DefaultHeartbeatInterval = 50 * time.Millisecond

	DefaultElectionTimeout = 150 * time.Millisecond
)

// ElectionTimeoutThresholdPercent specifies the threshold at which the server
// will dispatch warning events that the heartbeat RTT is too close to the
// election timeout.
const ElectionTimeoutThresholdPercent = 0.8

//------------------------------------------------------------------------------
//
// Errors
//
//------------------------------------------------------------------------------

var NotLeaderError = errors.New("raft.Server: Not current leader")
var DuplicatePeerError = errors.New("raft.Server: Duplicate peer")
var CommandTimeoutError = errors.New("raft: Command timeout")
var StopError = errors.New("raft: Has been stopped")


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}


type AppendEntriesArgs struct {
	Term int
	LeaderId int
	prevLogTerm int
	Entries LogEntry[]
	LeaderCommit int

}

type AppendEntriesReply struct {
	Term int
	Success bool
}

type ev struct {
	target      interface{}
	returnValue interface{}
	c           chan error
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	currentTerm int
	votedFor int
	logs []LogEntry
	commiteIndex int
	lastApplied int
	state ServerState
	leader int
	syncedPeer map[string]bool
	c *ev
	// only on leaders
	nextIndex []int
	matchIndex []int
	stopChan chan bool

}


func (rf *Raft) setState(st ServerState) {
	rf.mu.RLock()
	defer rf.mu.Unlock()
	prevState := rf.state
	prevLeader := rf.leader
	rf.state = st
	if state == Leader {
		rf.leader = rf.me
		rf.syncedPeer = make(map[string]bool)
	}
	if prevLeader != rf.leader {
		rf.DispatchEvent(newEvent(LeaderChangerEventType, rf.leader, prevLeader))
	}

}




// Generate a new election duration
func ElectionTimeout() float32 {

}


// Generate a new heartbeat duration
func NewHeartbeatDuration() float32 {

}

func (*rf Raft) CurrentState() ServerState {
	return rf.state
}




func (*rf Raft) Loop() {
	state := rf.CurrentState()
	for state != Stopped {
		switch state {
		case Follower:
			rf.follwerLoop()
		case Candidate:
			rf.candidateLoop()
		case Leader:
			rf.leaderLoop()
		}

	}
}

func (*rf Raft) followerLoop() {
	since := time.Now()
	electionTimeout := rf.ElectionTimeout()
	timeoutChan := afterBetween(electionTimeout, electionTimeout * 2)
	for rf.CurrentState() == Follower {
		var err error
		update := false
		select {
		case <- rf.stopped:
			rf.setState(Stopped)
			return
		case e := <-rf.c:
			switch req := e.target.(type) {
			//TODO COMMAND

			}
			e.c <- err

		case <-timeoutChan:
			if rf.promotable() {
				s.setState(Candidate)
			} else {
				update = true
			}

		}
		if update {
			since = time.Now()
			timeoutChan = afterBetween(electionTimeout, electionTimeout * 2)
		}
	}
}

//TODO
func (rf *Raft) GetLogLastInfo() {
	rf.mutex.Rlock()
	defer rf.mutex.RUnlock()

	if len(rf.logs) == 0 {
		return 0, 0
	}
	return 
}

func (rf *Raft) candidateLoop() {
	prevLeader := rf.leader
	rf.leader = -1
	lastLogIndex, lastLogTerm := rf.GetLogLastInfo()
	doVote := true
	votes := 0
	var timeoutChan <-chan time.Time
	var respChan chan *RequestVoteResponse
	for rf.CurrentState() == Candidate {
		if doVote {
			rf.currentTerm ++
			rf.votedFor = rf.me
			respChan = make(chan *RequestVoteResponse, len(rf.peers) - 1)
			for i, peer := range rf.peers {
				if i == rf.me {
					continue
				}
				rf.routineGroup.Add(1)
				go func(peer *Peer){
					defer rf.routineGroup.Done()
					peer.sendVoteRequest(newRequestVoteRequest(rf.currentTerm, rf.name, lastLogIndex, LastLogTerm), respChan)
				}(peer)

			}
			votes = 1
			timeoutChan = afterBetween(rf.ElectionTimeout(), rf.ElectionTimeout() * 2)
			doVote = False
		}

		if c == rf.QuorumSize() {
			rf.setState(Leader)
			return
		}
		select {
		case <-rf.stopped:
			rf.setState(Stopped)
			return

		case resp := <-respChan:
			if success: = rf.processVoteResponse(resp); success {
				candidateLoop++
			}

		case e := <-rf.c:
			var err error
			switch req:= e.target.(type) {
			case Command:
				err = NotLeaderError
			case *AppendEnriesRequest:
				e.returnValue, _ = rf.proccessAppendEntriesRequest(req)
			case *RequestVoteRequest:
				e.returnValue, _ = rf.processRequestVoteRequest(req)
			}
			e.c <- err

		case <-timeoutChan:
			doVote = true
		}
	}

}


func (s *Raft) leaderLoop() {
	logIndex, _ = s.lastInfo()
	for _, peer := range s.peers {
		peer.setPrevLogIndex(logIndex)
		peer.startHeartbeat()
	}

	for s.CurrentState() == Leader {
		var err error
		select {
		case <-s.stopped:
			for _, peer := range s.peers {
				peer.stopHeartbeat(false)
			}
			s.setState(Stopped)
			return
		case e := <-s.c:
			switch req := e.target.(type) {
			case Command:
				s.processCommand(req, e)
				continue
			case *AppendEntriesRequest:
				e.returnValue, _ = s.processAppendEntriesRequest(req)
			case *AppendEntriesResponse:
				s.processAppendEntriesResponse(req)
			case *RequestVoteRequest:
				e.returnValue, _ = s.processRequestVoteRequest(req)
			}
			e.c <- err
		}

	}

	s.syncedPeer = nil


}



func (s *server) snapshotLoop() {
	for s.State() == Snapshotting {
		var err error
		select {
		case <-s.stopped:
			s.setState(Stopped)
			return

		case e := <-s.c:
			switch req := e.target.(type) {
			case Command:
				err = NotLeaderError
			case *AppendEntriesRequest:
				e.returnValue, _ = s.processAppendEntriesRequest(req)
			case *RequestVoteRequest:
				e.returnValue, _ = s.processRequestVoteRequest(req)
			case *SnapshotRecoveryRequest:
				e.returnValue = s.processSnapshotRecoveryRequest(req)
			}
			// Callback to event.
			e.c <- err
		}
	}
}

 

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	return term, isleader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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


//
// restore previously persisted state.
//
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





//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}else{
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
		}else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false	
		}
	}

	rf.BecomeFollowerIfPossible(args.Term)
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
