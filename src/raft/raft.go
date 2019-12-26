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
	"errors"
	"fmt"
	"labgob"
	"labrpc"
	"sort"
	"sync"
	"time"
)

type ClientCommand struct {
	Command interface{}
}

type ClientCommandResp struct {
	Term  int
	Index int
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// ServerState
type ServerState int

const (
	Candidate ServerState = iota
	Follower
	Leader
	Stopped
	Initialized
)
const (
	// DefaultHeartbeatInterval is the interval that the leader will send
	// AppendEntriesRequests to followers to maintain leadership.
	DefaultHeartbeatInterval = 50 * time.Millisecond
	// DefaultElectionTimeout is the timeout that the follower set itself
	// to Candidate state
	DefaultElectionTimeout = 300 * time.Millisecond
)

type event struct {
	resp      interface{}
	eventType interface{}
	err       chan error
}

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
// Raft is a Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	Term     int
	VotedFor int
	leaderID int
	state    ServerState
	// communication channels
	stopChan          chan bool
	stopHeartbeatChan chan bool
	// logs entries
	logs RaftLog

	nextIndex  []int
	matchIndex []int
	applyCh    chan ApplyMsg
	// routine is a signal used to kill the server without interrupting other goroutines
	routine sync.WaitGroup

	// event channel
	eventChan chan *event
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.Term
	isleader = rf.state == Leader
	return term, isleader
}

func (rf *Raft) setState(st ServerState) {
	if st == rf.state {
		return
	}
	if rf.state == Leader {
		rf.stopHeartbeat()
	}
	rf.state = st
	if st == Leader {
		rf.leaderID = rf.me
	}

}

// State returns current state with lock
func (rf *Raft) State() ServerState {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

// quorumSize return majority members
func (rf *Raft) quorumSize() int {
	return len(rf.peers)/2 + 1
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:

	rf.routine.Add(1)
	defer rf.routine.Done()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	Term := rf.Term
	VotedFor := rf.VotedFor
	Entries := rf.logs.GetLogEntries()
	e.Encode(Term)
	e.Encode(VotedFor)
	e.Encode(Entries)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {

	if data == nil || len(data) == 0 { // bootstrap without any state?
		return
	}

	rf.routine.Add(1)
	defer rf.routine.Done()
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var Term int
	var VotedFor int
	var Entries []*LogEntry
	if d.Decode(&Term) != nil ||
		d.Decode(&VotedFor) != nil ||
		d.Decode(&Entries) != nil {
		panic("readPersist failed")
	} else {
		rf.VotedFor = VotedFor
		rf.Term = Term
		rf.logs.SetLogEntires(Entries)
	}
}

// RequestVoteArgs
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// AppendEntriesRequest
type AppendEntriesRequest struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []*LogEntry
	// debug
	LastIndex int
	LastTerm  int
}

// AppendEntriesReply
type AppendEntriesReply struct {
	Term      int
	Success   bool
	FromID    int
	NextIndex int
	// optimaztion
	ConflictTerm  int
	ConflictIndex int
}

// newElectTimeout produces a new election timeout between [DefaultElectionTimeout, DefaultElectionTimeout*2]
func (rf *Raft) newElectTimeout() <-chan time.Time {
	return afterBetween(DefaultElectionTimeout, DefaultElectionTimeout*2)
}

// sendRequestVote send rpc vote request to server
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	fmt.Printf("sendRequestVote %d %d\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.ProcessVoteRequest", args, reply)
	return ok
}

// ProcessVoteRequest
func (rf *Raft) ProcessVoteRequest(args *RequestVoteArgs, reply *RequestVoteReply) {
	ret, _ := rf.send(args)
	if resp, ok := ret.(*RequestVoteReply); ok {
		reply.VoteGranted = resp.VoteGranted
		reply.Term = resp.Term
	}

	return
}

func (rf *Raft) processVoteRequest(args *RequestVoteArgs) (*RequestVoteReply, bool) {
	reply := &RequestVoteReply{}
	if args.CandidateID == rf.me {
		panic("processVoteRequest its own")
	}
	lastIndex, lastTerm := rf.logs.LastInfo()
	println("processVoteRequest, CandidateID CandidateTerm me myTerm CandidateLastLogIndex CandidateLastLogTerm mylastIndex mylastTerm",
		args.CandidateID,
		args.Term,
		rf.me,
		rf.Term,
		args.LastLogIndex,
		args.LastLogTerm,
		lastIndex,
		lastTerm)
	// if the request is coming with an old term, reject it
	if args.Term < rf.Term {
		reply.VoteGranted = false
		reply.Term = rf.Term
		return reply, false
	}

	// if the request term is larger than this node,
	// update its current term and change it to FollowerState if possible
	// vote if it hasn't voted for others.
	if args.Term > rf.Term {
		rf.updateCurrentTerm(args.Term, -1)
	}
	reply.Term = rf.Term
	if rf.VotedFor != -1 && rf.VotedFor != args.CandidateID {
		reply.VoteGranted = false
		return reply, false
	}

	// lab 2C
	rf.persist()

	if lastTerm > args.LastLogTerm || (lastTerm == args.LastLogTerm && lastIndex > args.LastLogIndex) {
		reply.VoteGranted = false
		return reply, false
	}
	reply.VoteGranted = true
	rf.VotedFor = args.CandidateID
	return reply, true
}

// processVoteResponse
func (rf *Raft) processVoteResponse(reply *RequestVoteReply) bool {
	fmt.Printf("processVoteResponse %d\n", rf.me)
	if reply.VoteGranted && reply.Term == rf.Term {
		return true
	}
	if reply.Term > rf.Term {
		rf.updateCurrentTerm(reply.Term, -1)

		// lab 2C
		rf.persist()
	}
	return false
}

func (rf *Raft) sendAppendEntriesRequest(server int, args *AppendEntriesRequest, reply *AppendEntriesReply) bool {
	//fmt.Printf("sendAppendEntriesRequest %d %d %d \n", args.LeaderID, args.Term, server)
	ok := rf.peers[server].Call("Raft.ProcessAppendEntriesRequest", args, reply)
	return ok
}

// ProcessAppendEntriesRequest accepts rpc from remote
func (rf *Raft) ProcessAppendEntriesRequest(args *AppendEntriesRequest, reply *AppendEntriesReply) {
	ret, _ := rf.send(args)
	if resp, ok := ret.(*AppendEntriesReply); ok {
		reply.Success = resp.Success
		reply.Term = resp.Term
		reply.FromID = resp.FromID
		reply.NextIndex = resp.NextIndex
	}

	return
}

func (rf *Raft) applyToChan(fromIdx int, toIdx int) {
	println("applyToChan", rf.me, fromIdx, toIdx)
	// applyCh
	for c := fromIdx; c <= toIdx; c++ {
		committedEntry := rf.logs.GetLogEntry(c)
		rf.applyCh <- ApplyMsg{CommandValid: true, Command: committedEntry.Command, CommandIndex: committedEntry.Index}
	}
}

func (rf *Raft) processAppendEntriesRequest(args *AppendEntriesRequest) (*AppendEntriesReply, bool) {
	entry := rf.logs.GetLogEntry(args.PrevLogIndex)
	println("processAppendEntriesRequest leaderID, leaderTerm, myID, myTerm, LeaderPrevIndex, LeaderPrevTerm, MyIndex, MyTerm",
		args.LeaderID,
		args.Term,
		rf.me,
		rf.Term,
		args.PrevLogIndex,
		args.PrevLogTerm,
		entry.Index,
		entry.Term,
	)

	reply := &AppendEntriesReply{}
	reply.FromID = rf.me
	shouldPersist := false
	//fmt.Printf("follower entries %d\n"+s+"\n", rf.me)
	if args.Term < rf.Term {
		reply.Success = false
		reply.Term = rf.Term
		return reply, false
	} else {
		if args.Term > rf.Term {
			shouldPersist = true
		}
		rf.updateCurrentTerm(args.Term, args.LeaderID)

		// Reply false if log doesn’t contain an entry at prevLogIndex
		// whose term matches prevLogTerm
		if entry.Term != args.PrevLogTerm {

			reply.Success = false
			reply.Term = rf.Term
			// optimaztion: find the first index with the conflicting term
			entry := rf.logs.FindFirstEntryByTerm(entry.Term)
			reply.ConflictTerm = entry.Term
			reply.ConflictIndex = entry.Index
			return reply, true
		}
		//If an existing entry conflicts with a new one (same index
		// but different terms), delete the existing entry and all that
		// follow it

		for i := range args.Entries {
			entry = rf.logs.GetLogEntry(args.Entries[i].Index)
			fmt.Printf("Truncate %d %d %d %d\n", args.Entries[i].Index, args.Entries[i].Term, entry.Index, entry.Term)
			if entry.Term != args.Entries[i].Term {
				rf.logs.TruncateFromIndex(args.Entries[i].Index)
				// Append any new entries not already in the log
				for j := i; j < len(args.Entries); j++ {
					rf.logs.AppendCommand(args.Entries[j])
				}
				shouldPersist = true
				break
			}
		}
		if shouldPersist {
			// lab 2C
			rf.persist()
		}
		// If leaderCommit > commitIndex, set commitIndex =
		// min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.logs.CommitIndex() {
			prevCommitIndex := rf.logs.CommitIndex()
			lastIndex, _ := rf.logs.LastInfo()
			rf.logs.SetCommitIndex(MinInt(args.LeaderCommit, lastIndex))
			commitIndex := rf.logs.CommitIndex()
			rf.applyToChan(prevCommitIndex+1, commitIndex)

		}
		if len(args.Entries) > 0 {
			reply.NextIndex = args.Entries[len(args.Entries)-1].Index + 1
		} else {
			reply.NextIndex = args.PrevLogIndex + 1
		}
		reply.Success = true
		reply.Term = rf.Term

		return reply, true
	}
}

func (rf *Raft) processAppendEntriesResponse(reply *AppendEntriesReply) {
	fmt.Printf("processAppendEntriesResponse %d, %d\n", reply.FromID, rf.me)
	//
	if reply.Term > rf.Term {
		rf.updateCurrentTerm(reply.Term, -1)

		// lab 2C
		rf.persist()

		return
	}
	// If successful: update nextIndex and matchIndex for follower
	if reply.Success {
		rf.nextIndex[reply.FromID] = reply.NextIndex
		rf.matchIndex[reply.FromID] = reply.NextIndex - 1
		idx := rf.majorityCommitIndex()
		entry := rf.logs.GetLogEntry(idx)
		// If there exists an N such that N > commitIndex, a majority
		// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
		// set commitIndex = N
		if idx > rf.logs.CommitIndex() && entry.Term == rf.Term {
			rf.applyToChan(rf.logs.CommitIndex()+1, idx)
			rf.logs.SetCommitIndex(idx)
		}
	} else {
		rf.nextIndex[reply.FromID] = rf.logs.FindNextIndex(reply.ConflictIndex, reply.ConflictTerm)
	}

}

// send sends a event to server
func (rf *Raft) send(value interface{}) (interface{}, error) {

	event := &event{eventType: value, err: make(chan error, 1)}
	select {
	case rf.eventChan <- event:
	case <-rf.stopChan:
		return nil, StopError
	}
	select {
	case <-rf.stopChan:
		return nil, StopError
	case err := <-event.err:
		return event.resp, err
	}
}

// sendAsync sends a event to server in async way
func (rf *Raft) sendAsync(value interface{}) {

	event := &event{eventType: value, err: make(chan error, 1)}
	// try a non-blocking send first
	// in most cases, this should not be blocking
	// avoid create unnecessary go routines
	select {
	case rf.eventChan <- event:
		return
	default:
	}

	rf.routine.Add(1)
	go func() {
		defer rf.routine.Done()
		select {
		case rf.eventChan <- event:
		case <-rf.stopChan:
		}
	}()
}

func (rf *Raft) majorityCommitIndex() int {
	var indices []int
	idx, _ := rf.logs.LastInfo()
	indices = append(indices, idx)
	for i := range rf.matchIndex {
		if i == rf.me {
			continue
		}
		indices = append(indices, rf.matchIndex[i])
	}

	sort.Sort(sort.Reverse(sort.IntSlice(indices)))
	fmt.Printf("checkCommitIndex indices %d, %v\n", rf.me, indices)
	index := indices[len(rf.peers)/2]
	return index
}

//
// when current term is smaller than the request's term,
// update its own term and set it to Fowller state.
// assume lock already granted
func (rf *Raft) updateCurrentTerm(term int, leaderID int) {

	if term < rf.Term {
		panic("updateCurrentTerm rpc term is not smaller than current")

	}
	rf.Term = term
	rf.setState(Follower)
	rf.VotedFor = -1
	rf.leaderID = leaderID

}

// main loop
func (rf *Raft) loop() {
	state := rf.State()
	for state != Stopped {
		switch state {
		case Follower:
			rf.followerLoop()
		case Candidate:
			rf.candidateLoop()
		case Leader:
			rf.leaderLoop()
		}
		state = rf.State()
	}
}

func (rf *Raft) followerLoop() {

	timeoutChan := rf.newElectTimeout()

	for rf.State() == Follower {
		update := false

		select {
		case <-rf.stopChan:
			rf.setState(Stopped)
			return
		case e := <-rf.eventChan:
			var err error
			switch req := e.eventType.(type) {
			case *ClientCommand:
				err = NotLeaderError
			case *AppendEntriesRequest:
				e.resp, update = rf.processAppendEntriesRequest(req)
			case *RequestVoteArgs:
				e.resp, update = rf.processVoteRequest(req)
			}
			// Callback to event.
			e.err <- err
		case <-timeoutChan:
			rf.setState(Candidate)
		}
		//println("followerLoop", rf.me, update)
		if update {
			timeoutChan = rf.newElectTimeout()
		}
	}
}

func (rf *Raft) candidateLoop() {
	fmt.Printf("candidateLoop %d\n", rf.me)
	lastLogIndex, lastLogTerm := rf.logs.LastInfo()
	doVote := true
	votesGranted := 0
	var timeoutChan <-chan time.Time
	respChan := make(chan *RequestVoteReply, len(rf.peers))
	for rf.State() == Candidate {
		if doVote {
			votesGranted = 1
			rf.Term++
			rf.VotedFor = rf.me
			timeoutChan = rf.newElectTimeout()
			respChan = make(chan *RequestVoteReply, len(rf.peers))

			// Lab 2C, before sendRequestVote, persist
			rf.persist()

			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				rf.routine.Add(1)
				go func(peer int) {
					defer rf.routine.Done()
					reply := &RequestVoteReply{}
					if ok := rf.sendRequestVote(
						peer,
						&RequestVoteArgs{
							Term:         rf.Term,
							CandidateID:  rf.me,
							LastLogIndex: lastLogIndex,
							LastLogTerm:  lastLogTerm,
						},
						reply,
					); ok {
						respChan <- reply
					}

				}(i)
			}
			doVote = false
		}
		if votesGranted >= rf.quorumSize() {
			rf.setState(Leader)
			return
		}

		// Collect votes from peers.
		select {
		case <-rf.stopChan:
			rf.setState(Stopped)
			return

		case resp := <-respChan:
			if success := rf.processVoteResponse(resp); success {
				votesGranted++
				//fmt.Printf("get vote %d\n ", votesGranted)
			}

		case e := <-rf.eventChan:
			var err error
			switch req := e.eventType.(type) {
			case *ClientCommand:
				err = NotLeaderError
			case *AppendEntriesRequest:
				e.resp, _ = rf.processAppendEntriesRequest(req)
			case *RequestVoteArgs:
				e.resp, _ = rf.processVoteRequest(req)
			}
			// Callback to event.
			e.err <- err
		case <-timeoutChan:
			doVote = true
		}
	}
}

func (rf *Raft) leaderLoop() {
	fmt.Printf("leaderLoop %d\n", rf.me)
	// init nextIndex and matchIndex
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	lastIndex, _ := rf.logs.LastInfo()
	for i := range rf.nextIndex {
		rf.nextIndex[i] = lastIndex + 1

	}
	rf.startHeartbeat()
	// Begin to collect response from followers
	for rf.State() == Leader {
		var err error
		select {
		case <-rf.stopChan:
			rf.setState(Stopped)
			return

		case e := <-rf.eventChan:
			switch req := e.eventType.(type) {
			case *ClientCommand:
				e.resp = rf.processClientCommand(req)
			case *AppendEntriesRequest:
				e.resp, _ = rf.processAppendEntriesRequest(req)
			case *AppendEntriesReply:
				rf.processAppendEntriesResponse(req)
			case *RequestVoteArgs:
				e.resp, _ = rf.processVoteRequest(req)
			}

			// Callback to event.
			e.err <- err
		}
	}

}

func (rf *Raft) startHeartbeat() {
	rf.stopHeartbeatChan = make(chan bool)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			println("startHeartbeat", rf.me, peer)
			// send empty AppendEntriesRequest
			rf.mu.Lock()
			term := rf.Term
			rf.mu.Unlock()
			prevEntry := rf.logs.GetLogEntry(rf.nextIndex[peer] - 1)
			lastIndex, lastTerm := rf.logs.LastInfo()
			reply := &AppendEntriesReply{}
			if ok := rf.sendAppendEntriesRequest(
				peer,
				&AppendEntriesRequest{
					Term:         term,
					LeaderID:     rf.me,
					PrevLogIndex: prevEntry.Index,
					PrevLogTerm:  prevEntry.Term,
					LeaderCommit: rf.logs.CommitIndex(),
					Entries:      make([]*LogEntry, 0),
					LastIndex:    lastIndex,
					LastTerm:     lastTerm,
				},
				reply,
			); ok {
				rf.send(reply)
			}

			ticker := time.Tick(DefaultHeartbeatInterval)
			for {
				select {
				case <-rf.stopHeartbeatChan:
					println("stopHeartbeatChan", rf.me, peer)
					return
				case <-rf.stopChan:
					return
				case <-ticker:
					rf.mu.Lock()
					term := rf.Term
					rf.mu.Unlock()
					reply := &AppendEntriesReply{}
					var entries []*LogEntry
					prevEntry := rf.logs.GetLogEntry(rf.nextIndex[peer] - 1)
					lastIndex, lastTerm := rf.logs.LastInfo()
					if lastIndex < rf.nextIndex[peer] {
						entries = make([]*LogEntry, 0)
					} else {
						entries = rf.logs.GetLogsFromIndex(rf.nextIndex[peer])
					}
					if ok := rf.sendAppendEntriesRequest(
						peer,
						&AppendEntriesRequest{
							Term:         term,
							LeaderID:     rf.me,
							PrevLogIndex: prevEntry.Index,
							PrevLogTerm:  prevEntry.Term,
							LeaderCommit: rf.logs.CommitIndex(),
							Entries:      entries,
							LastIndex:    lastIndex,
							LastTerm:     lastTerm,
						},
						reply,
					); ok {
						rf.send(reply)
					}

				}
			}
		}(i)
	}
}

func (rf *Raft) stopHeartbeat() {
	close(rf.stopHeartbeatChan)
}

func (rf *Raft) processClientCommand(command *ClientCommand) *ClientCommandResp {
	logEntry := &LogEntry{Term: rf.Term, Command: command.Command}
	idx := rf.logs.AppendCommand(logEntry)

	// lab 2C
	rf.persist()

	return &ClientCommandResp{Index: idx, Term: rf.Term}
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

	ret, err := rf.send(&ClientCommand{Command: command})
	if err != nil {
		return -1, -1, false
	}
	resp, _ := ret.(*ClientCommandResp)
	return resp.Index, resp.Term, true

}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.

	close(rf.stopChan)
	rf.routine.Wait()
	rf.setState(Stopped)

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
	rf.leaderID = -1
	rf.Term = 0
	rf.VotedFor = -1
	// communication channels
	rf.stopChan = make(chan bool)
	rf.stopHeartbeatChan = make(chan bool)
	// initialize logs
	rf.logs = NewRaftLog()
	rf.applyCh = applyCh
	rf.eventChan = make(chan *event, 256)
	c := make(chan bool)
	// Your initialization code here (2A, 2B, 2C).
	go func(c chan bool) {
		c <- true
		rf.loop()
	}(c)
	// initialize from state persisted before a crash

	rf.readPersist(persister.ReadRaftState())
	rf.setState(Follower)
	<-c
	return rf
}
