package gron

import (
	"fmt"
	"time"
)

// ConstantDelaySchedule represents a simple recurring duty cycle, e.g. "Every 5 minutes".
// It does not support jobs more frequent than once a second.
type ConstantDelaySchedule struct {
	Delay time.Duration
}

// Every returns a crontab Schedule that activates once every duration.
// Delays of less than a second are not supported (will round up to 1 second).
// Any fields less than a Second are truncated.
func Every(duration time.Duration) ConstantDelaySchedule {
	if duration < time.Second {
		duration = time.Second
	}
	return ConstantDelaySchedule{
		Delay: duration - time.Duration(duration.Nanoseconds())%time.Second,
	}
}

// Next returns the next time this should be run.
// This rounds so that the next activation time will be on the second.
func (schedule ConstantDelaySchedule) Next(t time.Time) time.Time {
	// schedule.done = true
	return t.Add(schedule.Delay - time.Duration(t.Nanosecond())*time.Nanosecond)
}

type OnceDelaySchedule struct {
	Delay time.Duration
	Done  bool
}

func Once(duration time.Duration) OnceDelaySchedule {
	if duration < time.Second {
		duration = time.Second
	}
	return OnceDelaySchedule{
		Delay: duration - time.Duration(duration.Nanoseconds())%time.Second,
		Done:  false,
	}
}

func (schedule OnceDelaySchedule) Next(t time.Time) time.Time {
	if schedule.Done {
		fmt.Print(schedule.Done)
		return t
	}
	fmt.Print(schedule.Done)
	schedule.Done = true
	return t.Add(schedule.Delay - time.Duration(t.Nanosecond())*time.Nanosecond)
}
