package gron

import (
	"time"
)

type OnceDelaySchedule struct {
	Delay    time.Duration
	done bool
}

func Once(duration time.Duration) *OnceDelaySchedule {
	if duration < time.Second {
		duration = time.Second
	}
	return &OnceDelaySchedule{
		Delay:    duration - time.Duration(duration.Nanoseconds())%time.Second,
		done: false,
	}
}

func (s *OnceDelaySchedule) Next(t time.Time) time.Time {
	if s.done {
		return t
	}
	s.done = true
	return t.Add(s.Delay - time.Duration(t.Nanosecond())*time.Nanosecond)
}
