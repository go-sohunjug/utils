package event

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/go-sohunjug/utils/ants"
)

// Bus implements publish/subscribe messaging paradigm
type Bus interface {
	// Publish publishes arguments to the given topic subscribers
	// Publish block only when the buffer of one of the subscribers is full.
	Publish(topic string, args ...interface{})
	// Close unsubscribe all handlers from given topic
	Close(topic string)
	// Subscribe subscribes to the given topic
	Subscribe(topic string, fn interface{}) error
	// Unsubscribe unsubscribe handler from the given topic
	Unsubscribe(topic string, fn interface{}) error
}

type handlersMap map[string][]*handler

type handler struct {
	callback reflect.Value
	queue    chan []reflect.Value
}

type event struct {
	handlerQueueSize int
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

func (b *event) Subscribe(topic string, fn interface{}) error {
	if err := isValidHandler(fn); err != nil {
		return err
	}

	h := &handler{
		callback: reflect.ValueOf(fn),
		queue:    make(chan []reflect.Value, b.handlerQueueSize),
	}

	ants.Submit(func() {
		for args := range h.queue {
			h.callback.Call(args)
		}
	})

	b.mtx.Lock()
	defer b.mtx.Unlock()

	b.handlers[topic] = append(b.handlers[topic], h)

	return nil
}

func (b *event) Unsubscribe(topic string, fn interface{}) error {
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

func isValidHandler(fn interface{}) error {
	if reflect.TypeOf(fn).Kind() != reflect.Func {
		return fmt.Errorf("%s is not a reflect.Func", reflect.TypeOf(fn))
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
func New(handlerQueueSize int) Bus {
	if handlerQueueSize == 0 {
		panic("handlerQueueSize has to be greater then 0")
	}

	return &event{
		handlerQueueSize: handlerQueueSize,
		handlers:         make(handlersMap),
	}
}
