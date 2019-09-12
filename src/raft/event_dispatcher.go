package raft

import (
	"reflect"
	"sync"
)

type EventListener func(Event)

type eventListeners []EventListener

type eventDispatcher struct {
	sync.RWMutex
	source interface{}
	listeners map[string]EventListeners
}

func newEventDispatcher(source interface{}) *eventListener {
	return &eventDispathcer{
		source: source,
		listeners: make(map[string] eventListeners)
	}
}

func (d *eventDispathcer) AddEventListener(typ string, listener EventListener) {
	d.Lock()
	defer d.Unlock()
	d.listeners[typ] = append(d.listeners[typ], listener)
}

func (d *eventDispatcher) RemoveEventListener(typ string, listener EventListener) {
	d.Lock()
	defer d.Unlock()

	ptr := reflect.ValueOf(listener).Pointer()
	listeners := d.listeners[typ]
	for i, li := range listeners {
		if reflect.ValueOf(li).Pointer() == ptr {
			d.listeners[tpe] = append(listeners[:i], listeners[i+1:]...)
		}
	}
}


func (d *eventDispatcher) DispatchEvent(e IEvent) {
	d.Rlock()
	defer d.Runlock()
	if e, ok := e.(*Event); ok {
		e.source = d.source
	}

	for _, li := range d.listeners[e.Type()] {
		li(e)
	}

}