package event

import (
	"fmt"
	"log"
	"reflect"
	"sync"

	"github.com/go-sohunjug/utils/ants"
)

// Bus implements publish/subscribe messaging paradigm
type Bus interface {
	// Publish publishes arguments to the given topic subscribers
	// Publish block only when the buffer of one of the subscribers is full.
	Publish(topic string, args ...interface{})
	PublishTo(topic, name string, args ...interface{})
	// Close unsubscribe all handlers from given topic
	Close(topic string)
	// Subscribe subscribes to the given topic
	Subscribe(topic, name string, fn interface{}) error
	// Unsubscribe unsubscribe handler from the given topic
	UnsubscribeFn(topic string, fn interface{}) error
	Unsubscribe(topic, name string) error
	Stop()
}

type handlersMap map[string][]*handler

type handler struct {
	name     string
	callback reflect.Value
	queue    chan []reflect.Value
}

type event struct {
	handlerQueueSize int
	syncQueue        bool
	mtx              sync.RWMutex
	handlers         handlersMap
}

func (b *event) Publish(topic string, args ...interface{}) {
	rArgs := buildHandlerArgs(args)

	b.mtx.RLock()
	defer b.mtx.RUnlock()

	if hs, ok := b.handlers[topic]; ok {
		for _, h := range hs {
			h.queue <- rArgs
		}
	}
}

func (b *event) PublishTo(topic, name string, args ...interface{}) {
	rArgs := buildHandlerArgs(args)

	b.mtx.RLock()
	defer b.mtx.RUnlock()

	if hs, ok := b.handlers[topic]; ok {
		for _, h := range hs {
			if h.name == name {
				h.queue <- rArgs
				break
			}
		}
	}
}

func (b *event) Subscribe(topic, name string, fn interface{}) error {
	if err := isValidHandler(fn); err != nil {
		return err
	}
	if err := b.isValidName(topic, name); err != nil {
		return err
	}

	h := &handler{
		name:     name,
		callback: reflect.ValueOf(fn),
		queue:    make(chan []reflect.Value, b.handlerQueueSize),
	}

	ants.Submit(func() {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("Work failed with %s in %s", err, topic)
				b.Unsubscribe(topic, name)
			}
		}()
		for args := range h.queue {
			if b.syncQueue {
				h.callback.Call(args)
			} else {
				ants.Submit(func() {
					h.callback.Call(args)
				})
			}
		}
	})

	b.mtx.Lock()
	defer b.mtx.Unlock()

	b.handlers[topic] = append(b.handlers[topic], h)

	return nil
}

func (b *event) UnsubscribeFn(topic string, fn interface{}) error {
	if err := isValidHandler(fn); err != nil {
		return err
	}

	rv := reflect.ValueOf(fn)

	b.mtx.Lock()
	defer b.mtx.Unlock()

	if _, ok := b.handlers[topic]; ok {
		for i, h := range b.handlers[topic] {
			if h.callback == rv {
				close(h.queue)

				if len(b.handlers[topic]) == 1 {
					delete(b.handlers, topic)
				} else {
					b.handlers[topic] = append(b.handlers[topic][:i], b.handlers[topic][i+1:]...)
				}
			}
		}

		return nil
	}

	return fmt.Errorf("topic %s doesn't exist", topic)
}

func (b *event) Unsubscribe(topic, name string) error {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	if _, ok := b.handlers[topic]; ok {
		for i, h := range b.handlers[topic] {
			if h.name == name {
				close(h.queue)

				if len(b.handlers[topic]) == 1 {
					delete(b.handlers, topic)
				} else {
					b.handlers[topic] = append(b.handlers[topic][:i], b.handlers[topic][i+1:]...)
				}
			}
		}

		return nil
	}

	return fmt.Errorf("topic %s doesn't exist", topic)
}

func (b *event) Close(topic string) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	if _, ok := b.handlers[topic]; ok {
		for _, h := range b.handlers[topic] {
			close(h.queue)
		}

		delete(b.handlers, topic)

		return
	}
}

func (b *event) Stop() {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	for topic := range b.handlers {
		for _, h := range b.handlers[topic] {
			close(h.queue)
		}
		delete(b.handlers, topic)
	}
}

func isValidHandler(fn interface{}) error {
	if reflect.TypeOf(fn).Kind() != reflect.Func {
		return fmt.Errorf("%s is not a reflect.Func", reflect.TypeOf(fn))
	}

	return nil
}

func (b *event) isValidName(topic, name string) error {
	if hs, ok := b.handlers[topic]; ok {
		for _, h := range hs {
			if h.name == name {
				return fmt.Errorf("%s is already in %s", name, topic)
			}
		}
	}
	return nil
}

func buildHandlerArgs(args []interface{}) []reflect.Value {
	reflectedArgs := make([]reflect.Value, 0)

	for _, arg := range args {
		reflectedArgs = append(reflectedArgs, reflect.ValueOf(arg))
	}

	return reflectedArgs
}

// New creates new MessageBus
// handlerQueueSize sets buffered channel length per subscriber
func New(handlerQueueSize int, syncQueue bool) Bus {
	if handlerQueueSize <= 0 {
		panic("handlerQueueSize has to be greater then 0")
	}

	return &event{
		syncQueue:        syncQueue,
		handlerQueueSize: handlerQueueSize,
		handlers:         make(handlersMap),
	}
}
