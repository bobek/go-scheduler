/*
Package scheduler implements a simple scheduler for scheduling multiple
recurring jobs.

Please check AddSchedule() for details. Scheduler will try to compensate for
delays and amount of time needed for executing the job. This means, that time
between runs will be actually smaller then `interval`.
*/
package scheduler

import (
	"time"
)

// Worker interface for writing the actual job to be executed according to the Schedule.
type Worker interface {
	PerformWork()
}

type confirmation bool

// Scheduler is wrapper around multiple Schedules. They will be executed one
// after another. E.g. Scheduler currently enforces serialization through having only
// one execution worker.
//
// We may add things like graceful shutdown in the future.
type Scheduler struct {
	schedules []*Schedule
	jobch     chan *Schedule
}

// Schedule is one piece of fork to be executed ever `interval`. Doing work means
// that `PerformWork()` is called on the `worker`.
type Schedule struct {
	interval            time.Duration
	worker              Worker
	confirmationChannel chan confirmation
}

// New creates a new Scheduler and spawns scheduling process.
func New() *Scheduler {
	scheduler := &Scheduler{
		jobch: make(chan *Schedule, 1),
	}
	// We have only one consumer on job channel -> serializing all the jobs
	go scheduler.workerLoop()
	return scheduler
}

func (scheduler *Scheduler) workerLoop() {
	for {
		schedule := <-scheduler.jobch
		schedule.worker.PerformWork()
		schedule.confirmationChannel <- true // Confirm work being done after return from worker
	}
}

/*
AddSchedule adds a new Schedule to existing Scheduler.

- worker: anything what implements Worker interface.
- interval: time.Duration between runs of passed worker.

Provided `worker` will be called every `interval`. Scheduler will try to
compensate for delays. So the interval between runs will be actually shorter.

For example, let's assume, that schedule was started at 16:00:00 with interval
of 20s:
	16:00:00 1st execution took 5s -> scheduled to run after 15s
	16:00:20 2nd execution took 10s -> scheduled to run after 10s
	16:00:30 3rd execution took 1s -> scheduled to run after 19s
*/
func (scheduler *Scheduler) AddSchedule(worker Worker, interval time.Duration) {
	schedule := &Schedule{
		worker:              worker,
		interval:            interval,
		confirmationChannel: make(chan confirmation, 1),
	}
	scheduler.schedules = append(scheduler.schedules, schedule)
	go schedule.scheduleLoop(scheduler.jobch)
}

func (s *Schedule) scheduleLoop(jobch chan *Schedule) {
	timer := time.NewTimer(time.Nanosecond)
	startTime := time.Now()
	for {
		select {
		case <-timer.C:
			// Fetching startTime before sending to channel means, that we will see how long we have been waiting.
			startTime = time.Now()
			jobch <- s

		// We have received a confirmation of the work being finished, thus we can
		// schedule another run. We try to compensate for time spent
		// waiting/processing.
		case <-s.confirmationChannel:
			passedTime := time.Since(startTime)
			waitTime := s.interval - passedTime
			if waitTime < 0 {
				waitTime = time.Nanosecond
			}
			timer.Reset(waitTime)
		}
	}
}
