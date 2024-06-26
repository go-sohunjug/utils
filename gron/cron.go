package gron

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/go-sohunjug/utils/ants"
	"github.com/google/uuid"
)

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
	entries   []*Entry
	chain     Chain
	stop      chan struct{}
	add       chan *Entry
	change    chan *Entry
	remove    chan interface{}
	running   bool
	logger    Logger
	runningMu *sync.Mutex
	location  *time.Location
	parser    ScheduleParser
	nextID    EntryID
	jobWaiter sync.WaitGroup
	timer     *time.Timer
}

// ScheduleParser is an interface for schedule spec parsers that return a Schedule
type ScheduleParser interface {
	Parse(spec string) (Schedule, error)
}

// Job is an interface for submitted cron jobs.
type Job interface {
	Run()
	Name() string
}

// Schedule describes a job's duty cycle.
type Schedule interface {
	// Next returns the next activation time, later than the given time.
	// Next is invoked initially, and then each time the job is run.
	Next(time.Time) time.Time
}

// EntryID identifies an entry within a Cron instance
type EntryID int

// Entry consists of a schedule and the func to execute on that schedule.
type Entry struct {
	// ID is the cron-assigned ID of this entry, which may be used to look up a
	c  *Cron
	ID EntryID

	Name string

	// Schedule on which this job should be run.
	Schedule Schedule

	// Next time the job will run, or the zero time if Cron has not been
	// started or this entry's schedule is unsatisfiable
	Next time.Time

	// Prev is the last time this job was run, or the zero time if never.
	Prev time.Time

	// WrappedJob is the thing to run when the Schedule is activated.
	WrappedJob Job

	// Job is the thing that was submitted to cron.
	// It is kept around so that user code that needs to get at the job later,
	// e.g. via Entries() can do so.
	Job Job
}

// Valid returns true if this is not the zero entry.
func (e Entry) Valid() bool { return e.ID != 0 }

// byTime is a wrapper for sorting the entry array by time
// (with zero time at the end).
type byTime []*Entry

func (s byTime) Len() int      { return len(s) }
func (s byTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byTime) Less(i, j int) bool {
	// Two zero times should return false.
	// Otherwise, zero is "greater" than any other time.
	// (To sort it at the end of the list.)
	if s[i].Next.IsZero() {
		return false
	}
	if s[j].Next.IsZero() {
		return true
	}
	return s[i].Next.Before(s[j].Next)
}

// New returns a new Cron job runner, modified by the given options.
//
// Available Settings
//
//	Time Zone
//	  Description: The time zone in which schedules are interpreted
//	  Default:     time.Local
//
//	Parser
//	  Description: Parser converts cron spec strings into cron.Schedules.
//	  Default:     Accepts this spec: https://en.wikipedia.org/wiki/Cron
//
//	Chain
//	  Description: Wrap submitted jobs to customize behavior.
//	  Default:     A chain that recovers panics and logs them to stderr.
//
// See "cron.With*" to modify the default behavior.
func New(opts ...Option) *Cron {
	c := &Cron{
		entries:   nil,
		chain:     NewChain(),
		add:       make(chan *Entry, 20),
		stop:      make(chan struct{}, 20),
		change:    make(chan *Entry, 20),
		remove:    make(chan interface{}, 20),
		running:   false,
		runningMu: &sync.Mutex{},
		logger:    DefaultLogger,
		location:  time.Local,
		parser:    standardParser,
		timer:     nil,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// FuncJob is a wrapper that turns a func() into a cron.Job
type FuncJob struct {
	n string
	f func()
}

func (f FuncJob) Run() { f.f() }

func (f FuncJob) Name() string { return f.n }

// AddFunc adds a func to the Cron to be run on the given schedule.
// The spec is parsed using the time zone of this Cron instance as the default.
// An opaque ID is returned that can be used to later remove it.
func (c *Cron) AddFunc(spec string, cmd func()) *Entry {
	schedule, err := c.parser.Parse(spec)
	if err != nil {
		return nil
	}
	uuidV1, _ := uuid.NewUUID()
	return c.AddJob(schedule, uuidV1.String(), cmd)
}

func (c *Cron) AddHandlerFuncSync(name, spec string, cmd func()) *Entry {
	schedule, err := c.parser.Parse(spec)
	if err != nil {
		return nil
	}
	return c.AddJob(schedule, name, cmd)
}

func (c *Cron) AddHandlerFunc(name, spec string, cmd func()) {
	schedule, err := c.parser.Parse(spec)
	if err != nil {
		return
	}
	c.AddJob(schedule, name, cmd)
}

func (c *Cron) HandlerReset(name string, args ...any) {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	entry := c.Entry(name)
	if c.Entry(name) == nil {
		return
	}
	ants.Submit(func() {
		entry.Update(args...)
	})
	return
}

// Schedule adds a Job to the Cron to be run on the given schedule.
// The job is wrapped with the configured Chain.
func (c *Cron) AddJob(schedule Schedule, name string, f func()) *Entry {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	entry := c.Entry(name)
	if c.Entry(name) != nil {
		return entry
	}
	c.nextID++
	cmd := FuncJob{name, f}
	entry = &Entry{
		c:          c,
		ID:         c.nextID,
		Name:       name,
		Schedule:   schedule,
		WrappedJob: c.chain.Then(cmd),
		Job:        cmd,
	}
	if !c.running {
		c.entries = append(c.entries, entry)
	} else {
		c.add <- entry
	}
	return entry
}

func (e *Entry) Update(args ...any) {
	schedule_flag := false
	for _, arg := range args {
		if v, ok := arg.(string); ok {
			schedule, err := e.c.parser.Parse(v)
			if err != nil {
				e.Name = v
			} else {
				e.Schedule = schedule
				schedule_flag = true
			}
		} else if v, ok := arg.(EntryID); ok {
			e.ID = v
		} else if v, ok := arg.(func()); ok {
			e.Job = FuncJob{e.Name, v}
		} else if v, ok := arg.(Job); ok {
			e.Job = v
		}
	}
	if schedule_flag {
		if e.c.running {
			e.c.change <- e
		}
	}
}

func (c *Cron) Entries() []*Entry {
	return c.entries
}

// Location gets the time zone location
func (c *Cron) Location() *time.Location {
	return c.location
}

func (c *Cron) Entry(id any) *Entry {
	for _, entry := range c.entries {
		if value, ok := id.(EntryID); ok {
			if value == entry.ID {
				return entry
			}
		} else if value, ok := id.(string); ok {
			if value == entry.Name {
				return entry
			}
		}
	}
	return nil
}

// Remove an entry from being run in the future.

func (c *Cron) Remove(id any) {
	if c.running {
		c.remove <- id
	} else {
		c.removeEntry(id)
	}
}

// Start the cron scheduler in its own goroutine, or no-op if already started.
func (c *Cron) Start() {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		return
	}
	c.running = true
	ants.Submit(c.run)
}

// Run the cron scheduler, or no-op if already running.
func (c *Cron) Run() {
	c.runningMu.Lock()
	if c.running {
		c.runningMu.Unlock()
		return
	}
	c.running = true
	c.runningMu.Unlock()
	c.run()
}

// run the scheduler.. this is private just due to the need to synchronize
// access to the 'running' state variable.
func (c *Cron) run() {
	c.logger.Infof("start")

	// Figure out the next activation times for each entry.
	now := c.now()
	for _, entry := range c.entries {
		entry.Next = entry.Schedule.Next(now)
		c.logger.Infof("schedule", "now", now, "entry", entry.ID, "next", entry.Next)
	}

	for {
		// Determine the next entry to run.
		sort.Sort(byTime(c.entries))

		if len(c.entries) == 0 || c.entries[0].Next.IsZero() {
			// If there are no entries yet, just sleep - it still handles new entries
			// and stop requests.
			c.timer = time.NewTimer(100000 * time.Hour)
		} else {
			c.timer = time.NewTimer(c.entries[0].Next.Sub(now))
		}

		// for {
		select {
		case now = <-c.timer.C:
			now = now.In(c.location)
			c.logger.Infof("wake", "now", now)

			// Run every entry whose next time was less than now
			for _, e := range c.entries {
				if e.Next.After(now) || e.Next.IsZero() {
					break
				}
				c.startJob(e.WrappedJob)
				e.Prev = e.Next
				e.Next = e.Schedule.Next(now)
				c.logger.Infof("run", "now", now, "entry", e.Name, "next", e.Next, "schedule", e.Schedule)
				if e.Next.Equal(now) {
					c.removeEntry(e.Name)
					break
				}
			}

		case entry := <-c.change:
			c.timer.Stop()
			now = c.now()
			entry.Next = entry.Schedule.Next(now)
			c.logger.Infof("update", "now", now, "entry", entry.Name, "next", entry.Next)

		case newEntry := <-c.add:
			c.timer.Stop()
			now = c.now()
			newEntry.Next = newEntry.Schedule.Next(now)
			c.entries = append(c.entries, newEntry)
			c.logger.Infof("added", "now", now, "entry", newEntry.ID, newEntry.Name, "next", newEntry.Next)

		case <-c.stop:
			c.timer.Stop()
			c.logger.Infof("stop")
			return

		case id := <-c.remove:
			c.timer.Stop()
			now = c.now()
			c.removeEntry(id)
			c.logger.Infof("removed", "entry", id)
		}
		// 	break
		// }
	}
}

// startJob runs the given job in a new goroutine.
func (c *Cron) startJob(j Job) {
	c.jobWaiter.Add(1)
	defer c.jobWaiter.Done()
	ants.Submit(func() {
		j.Run()
	})
}

// now returns current time in c location
func (c *Cron) now() time.Time {
	return time.Now().In(c.location)
}

// Stop stops the cron scheduler if it is running; otherwise it does nothing.
// A context is returned so the caller can wait for running jobs to complete.
func (c *Cron) Stop() context.Context {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		c.stop <- struct{}{}
		c.running = false
	}
	ctx, cancel := context.WithCancel(context.Background())
	ants.Submit(func() {
		c.jobWaiter.Wait()
		cancel()
	})
	return ctx
}

func (c *Cron) removeEntry(id any) {
	var entries []*Entry
	for _, e := range c.entries {
		if value, ok := id.(EntryID); ok {
			if value != e.ID {
				entries = append(entries, e)
			}
		} else if value, ok := id.(string); ok {
			if value != e.Name {
				entries = append(entries, e)
			}
		} else if value, ok := id.(Entry); ok {
			if value.ID != e.ID || value.Name != e.Name {
				entries = append(entries, e)
			}
		}
	}
	c.entries = entries
}
