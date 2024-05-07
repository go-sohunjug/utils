package gron

import (
	"fmt"
	"sort"
	"time"

	"github.com/go-sohunjug/utils/ants"
	"github.com/google/uuid"
)

// Entry consists of a schedule and the job to be executed on that schedule.
type Entry struct {
	Schedule Schedule
	Job      Job
	Name     string

	// the next time the job will run. This is zero time if Cron has not been
	// started or invalid schedule.
	Next time.Time

	// the last time the job was run. This is zero time if the job has not been
	// run.
	Prev time.Time
}

// byTime is a handy wrapper to chronologically sort entries.
type byTime []*Entry

func (b byTime) Len() int      { return len(b) }
func (b byTime) Swap(i, j int) { b[i], b[j] = b[j], b[i] }

// Less reports `earliest` time i should sort before j.
// zero time is not `earliest` time.
func (b byTime) Less(i, j int) bool {
	if b[i].Next.IsZero() {
		return false
	}
	if b[j].Next.IsZero() {
		return true
	}

	return b[i].Next.Before(b[j].Next)
}

// Job is the interface that wraps the basic Run method.
//
// Run executes the underlying func.
type Job interface {
	Run()
}

// Cron provides a convenient interface for scheduling job such as to clean-up
// database entry every month.
//
// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may also be started, stopped and the entries
// may be inspected.
type Cron struct {
	entries []*Entry
	running bool
	pool    ants.Pool
	add     chan *Entry
	stop    chan string
}

// New instantiates new Cron instant c.
func New() *Cron {
	return &Cron{
		stop:    make(chan string),
		add:     make(chan *Entry),
		running: false,
	}
}

// Start signals cron instant c to get up and running.
func (c *Cron) Start() {
	if !c.running {
		c.running = true
		ants.Submit(c.run)
	}
}

// Add appends schedule, job to entries.
//
// if cron instant is not running, adding to entries is trivial.
// otherwise, to prevent data-race, adds through channel.
func (c *Cron) Add(s Schedule, j Job) string {
  uuidV1, _ := uuid.NewUUID()
	entry := &Entry{
		Schedule: s,
		Job:      j,
    Name: uuidV1.String(),
	}

	if !c.running {
		c.entries = append(c.entries, entry)
		return uuidV1.String()
	}
	c.add <- entry
  return uuidV1.String()
}

// AddFunc registers the Job function for the given Schedule.
func (c *Cron) AddFunc(s Schedule, j func()) string {
	return c.Add(s, JobFunc(j))
}

// Add appends schedule, job to entries.
//
// if cron instant is not running, adding to entries is trivial.
// otherwise, to prevent data-race, adds through channel.
func (c *Cron) AddHanderJob(n string, s Schedule, j Job) (err error) {
	for _, e := range c.entries {
		if e.Name == n {
			err = fmt.Errorf("hander %s already in scheduler", n)
			return
		}
	}
	entry := &Entry{
		Schedule: s,
		Job:      j,
		Name:     n,
	}

	if !c.running {
		c.entries = append(c.entries, entry)
		return
	}
	c.add <- entry
	return
}

// AddFunc registers the Job function for the given Schedule.
func (c *Cron) AddHandlerFunc(n string, s Schedule, j func()) (err error) {
	err = c.AddHanderJob(n, s, JobFunc(j))
	return
}

func (c *Cron) RemoveHandler(n string) {
	if !c.running {
		return
	}
	c.stop <- n
}

// Stop halts cron instant c from running.
func (c *Cron) Stop() {
	if !c.running {
		return
	}
	c.running = false
	c.stop <- "_all_jobs_"
}

var after = time.After

// run the scheduler...
//
// It needs to be private as it's responsible of synchronizing a critical
// shared state: `running`.
func (c *Cron) run() {
	var effective time.Time
	now := time.Now().Local()

	// to figure next trig time for entries, referenced from now
	for _, e := range c.entries {
		e.Next = e.Schedule.Next(now)
	}

	for {
		sort.Sort(byTime(c.entries))
		if len(c.entries) > 0 {
			effective = c.entries[0].Next
		} else {
			effective = now.AddDate(15, 0, 0) // to prevent phantom jobs.
		}

		select {
		case now = <-after(effective.Sub(now)):
			// entries with same time gets run.
			for _, entry := range c.entries {
				if entry.Next != effective {
					break
				}
				entry.Prev = now
				entry.Next = entry.Schedule.Next(now)
				ants.Submit(entry.Job.Run)
			}
		case e := <-c.add:
			e.Next = e.Schedule.Next(time.Now())
			c.entries = append(c.entries, e)
		case n := <-c.stop:
			if n == "_all_jobs_" {
				return // terminate go-routine.
			}
			for i, e := range c.entries {
				if e.Name == n {
					c.entries = append(c.entries[:i], c.entries[i+1:]...)
					break
				}
			}
		}
	}
}

func (c *Cron) List() (list []string) {
	for _, e := range c.entries {
		list = append(list, e.Name)
	}
	return list
}

// Entries returns cron etn
func (c *Cron) Entries() []*Entry {
	return c.entries
}

// JobFunc is an adapter to allow the use of ordinary functions as gron.Job
// If f is a function with the appropriate signature, JobFunc(f) is a handler
// that calls f.
//
// todo: possibly func with params? maybe not needed.
type JobFunc func()

// Run calls j()
func (j JobFunc) Run() {
	j()
}
