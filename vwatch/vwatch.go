package vwatch

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"reflect"
	"time"

	"github.com/go-sohunjug/utils/ants"
)

type Watcher struct {
	ptr      interface{}
	interval time.Duration
	last     map[string]string
	nodes    map[string]func()
	ticker   *time.Ticker
}

func NewWatcher(v interface{}, interval time.Duration) *Watcher {
	w := &Watcher{
		ptr:      v,
		interval: interval,
		last:     make(map[string]string),
		nodes:    make(map[string]func()),
	}
	w.run()
	return w
}

func (t *Watcher) Watch(tag string, f func()) *Watcher {
	t.nodes[tag] = f
	return t
}

func (t *Watcher) run() {
	t.ticker = time.NewTicker(t.interval)
	ants.Submit(func() {
		for {
			<-t.ticker.C
			if t.interval < 10*time.Second {
				ants.Submit(t.do)
			} else {
				t.do()
			}
		}
	})
}

func (t *Watcher) Stop() {
	if t.ticker == nil {
		return
	}
	t.ticker.Stop()
	t.ticker = nil
}

func (t *Watcher) Start() {
	if t.ticker == nil {
		t.run()
	}
}

func (t *Watcher) Reset(interval time.Duration) {
	t.interval = interval
	if t.ticker == nil {
		return
	}
	t.ticker.Reset(t.interval)
}

func (t *Watcher) trigger(tag string) {
	if f, ok := t.nodes[tag]; ok {
		f()
	}
}

func (t *Watcher) do() {
	data := make(map[string]string)
	extract(reflect.ValueOf(t.ptr), data)
	for k, v := range data {
		if _, ok := t.last[k]; !ok {
			continue
		}
		if v != t.last[k] {
			t.trigger(k)
		}
	}
	t.last = data
}

func checkKind(k reflect.Kind) bool {
	for _, kind := range []reflect.Kind{reflect.Struct, reflect.Map, reflect.Slice, reflect.Array, reflect.Ptr} {
		if k == kind {
			return true
		}
	}
	return false
}

//	func extract(val reflect.Value, data map[string]string) {
//		switch val.Kind() {
//		case reflect.Ptr:
//			elem := val.Elem()
//			if !elem.CanAddr() {
//				return
//			}
//
//			if elem.CanInterface() {
//				extract(reflect.ValueOf(elem.Interface()), data)
//			}
//			if checkKind(elem.Kind()) {
//				extract(elem, data)
//			}
//			break
//		case reflect.Struct:
//			for i := 0; i < val.NumField(); i++ {
//				tag := val.Type().Field(i).Tag.Get("vwatch")
//				if tag != "" && tag != "-" && tag != "_" {
//					if val.Field(i).CanInterface() {
//						data[tag] = hash(val.Field(i).Interface())
//					} else {
//						data[tag] = hash(val.Field(i))
//					}
//				}
//				if !val.Field(i).CanInterface() {
//					if checkKind(val.Field(i).Kind()) {
//						extract(val.Field(i), data)
//					}
//					continue
//				}
//				extract(reflect.ValueOf(val.Field(i).Interface()), data)
//			}
//			break
//		case reflect.Map:
//			iter := val.MapRange()
//			for iter.Next() {
//				extract(reflect.ValueOf(iter.Value().Interface()), data)
//			}
//			break
//		case reflect.Slice, reflect.Array:
//			for i := 0; i < val.Len(); i++ {
//				if val.Index(i).CanInterface() {
//					extract(reflect.ValueOf(val.Index(i).Interface()), data)
//				} else {
//					extract(reflect.ValueOf(val.Index(i)), data)
//				}
//			}
//			break
//		}
//	}
func extract(val reflect.Value, data map[string]string) {
	switch val.Kind() {
	case reflect.Ptr:
		elem := val.Elem()
		if !elem.CanAddr() {
			return
		}
		extract(reflect.ValueOf(elem.Interface()), data)
		break
	case reflect.Struct:
		for i := 0; i < val.NumField(); i++ {
			if !val.Field(i).CanInterface() {
				continue
			}
			tag := val.Type().Field(i).Tag.Get("vwatch")
			if tag != "" && tag != "-" && tag != "_" {
				data[tag] = hash(val.Field(i).Interface())
			}
			extract(reflect.ValueOf(val.Field(i).Interface()), data)
		}
		break
	case reflect.Map:
		iter := val.MapRange()
		for iter.Next() {
			extract(reflect.ValueOf(iter.Value().Interface()), data)
		}
		break
	case reflect.Slice, reflect.Array:
		for i := 0; i < val.Len(); i++ {
			extract(reflect.ValueOf(val.Index(i).Interface()), data)
		}
		break
	}
}

func hash(i interface{}) string {
	s := fmt.Sprintf("%#v", i)
	h := md5.New()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}
