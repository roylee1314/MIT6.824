package raft

import (
	"fmt"
	"sync"
)

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

func (e *LogEntry) deepcopy() *LogEntry {
	return &LogEntry{Command: e.Command, Term: e.Term, Index: e.Index}
}

type RaftLog interface {
	AppendCommand(*LogEntry) int
	LastInfo() (int, int)
	GetLogsFromIndex(int) []*LogEntry
	GetLogEntry(int) *LogEntry
	TruncateFromIndex(int) bool
	SetCommitIndex(int)
	CommitIndex() int
	PrintEntries() string
	GetLogEntries() []*LogEntry
	SetLogEntires([]*LogEntry)
	Length() int
	FindFirstEntryByTerm(int) *LogEntry
	FindNextIndex(int, int) int
}

type raftLog struct {
	mu          sync.RWMutex
	Entries     []*LogEntry
	commitIndex int
}

func (lg *raftLog) AppendCommand(entry *LogEntry) int {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	entry.Index = len(lg.Entries)
	lg.Entries = append(lg.Entries, entry)
	return entry.Index
}

func (lg *raftLog) LastInfo() (int, int) {
	lg.mu.RLock()
	defer lg.mu.RUnlock()
	e := lg.Entries[len(lg.Entries)-1]
	return e.Index, e.Term
}

func (lg *raftLog) GetLogsFromIndex(index int) []*LogEntry {
	lg.mu.RLock()
	defer lg.mu.RUnlock()
	values := make([]*LogEntry, 0)
	for i := index; i < len(lg.Entries); i++ {
		values = append(values, lg.Entries[i])
	}
	return values
}

func (lg *raftLog) GetLogEntry(idx int) *LogEntry {
	lg.mu.RLock()
	defer lg.mu.RUnlock()
	if idx <= 0 || idx >= len(lg.Entries) {
		return &LogEntry{Term: 0, Index: 0}
	}
	return lg.Entries[idx]
}

func (lg *raftLog) TruncateFromIndex(idx int) bool {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	if idx <= 0 || idx > len(lg.Entries) {
		return false
	}
	lg.Entries = lg.Entries[:idx]
	return true
}

func (lg *raftLog) SetCommitIndex(index int) {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	if index <= lg.commitIndex {
		return
	}
	lg.commitIndex = index

}

func (lg *raftLog) CommitIndex() int {
	lg.mu.RLock()
	defer lg.mu.RUnlock()
	return lg.commitIndex
}

func (lg *raftLog) PrintEntries() string {
	lg.mu.RLock()
	defer lg.mu.RUnlock()
	s := ""
	for i := range lg.Entries[1:] {

		s += fmt.Sprintf("entry index: %d, cmd %d, term: %d ", lg.Entries[i].Index, lg.Entries[i].Command.(int), lg.Entries[i].Term)
	}
	return s
}

func (lg *raftLog) GetLogEntries() []*LogEntry {
	lg.mu.RLock()
	defer lg.mu.RUnlock()
	return lg.Entries
}

func (lg *raftLog) SetLogEntires(e []*LogEntry) {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	lg.Entries = e
}
func (lg *raftLog) Length() int {
	return len(lg.Entries) - 1
}

func (lg *raftLog) FindFirstEntryByTerm(term int) *LogEntry {
	for _, entry := range lg.Entries {
		if entry.Term == term {
			return entry
		}
	}
	return lg.Entries[0]
}

func (lg *raftLog) FindNextIndex(index int, term int) int {
	for i := index; i < len(lg.Entries); i++ {
		if lg.Entries[i].Term != term {
			return i
		}
	}
	return len(lg.Entries)
}

func NewRaftLog() *raftLog {
	log := &raftLog{}
	log.Entries = make([]*LogEntry, 0)
	// placeholder
	log.Entries = append(log.Entries, &LogEntry{})
	log.commitIndex = 0
	return log
}
