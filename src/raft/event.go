package raft

const (
	StateChangeEventType  = "stateChange"
	LeaderChangeEventType = "leaderChange"
	TermChangeEventType   = "termChange"
	CommitEventType   = "commit"
	AddPeerEventType      = "addPeer"
	RemovePeerEventType   = "removePeer"

	HeartbeatIntervalEventType        = "heartbeatInterval"
	ElectionTimeoutThresholdEventType = "electionTimeoutThreshold"

	HeartbeatEventType = "heartbeat"
)

type IEvent interface {
	Type() string
	Source() interface{}
	Value() interface{}
	PrevValue() interface{}
}

type Event struct {
	typ       string
	source    interface{}
	value     interface{}
	prevValue interface{}
}

func newEvent(typ string, value interface{}, prevValue interface{}) *Event {
	return &Event{
		typ: typ,
		value: value,
		prevValue: prevValue,
	}
}

func (e *Event) Type() string {
	return e.typ
}

func (e *Event) Source() interface{} {
	return e.source
}

func (e *Event) Value() interface{} {
	return e.value
}

func (e *Event) PrevValue() interface{} {
	return e.prevValue
}