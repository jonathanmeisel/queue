package worker

import (
	"context"
	"database/sql"
	"errors"
	_ "errors"
	"sync"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

type Worker struct {
	uuid     uuid.UUID                                                // session UUID for this worker
	callback func(context.Context, []byte, func(*sql.Tx) error) Error // callback function to perform work
	options  *Options

	workerDone    chan bool // channel used to signal this worker has been closed
	heartbeatDone chan bool

	// tickers: one for the heartbeat updater, another for work polling
	heartbeatTicker *time.Ticker
	workPollTicker  *time.Ticker

	// Wait for goroutines to finish when we close
	workerWaitGroup    sync.WaitGroup
	heartbeatWaitGroup sync.WaitGroup

	db *sql.DB
}

type Options struct {
	PollInterval      time.Duration
	HeartbeatInterval time.Duration
	Deadline          time.Duration
}

// Use like:
// NewOptions().WithPollInterval(pollInterval).WithDeadline(deadline).WithHeartbeat(heartbeat)
func NewOptions() *Options {
	pollInterval, _ := time.ParseDuration("500ms")
	heartbeatInterval, _ := time.ParseDuration("1s")
	deadline, _ := time.ParseDuration("5s")

	return &Options{
		PollInterval:      pollInterval,
		HeartbeatInterval: heartbeatInterval,
		Deadline:          deadline,
	}
}

func (this *Options) WithPollInterval(duration time.Duration) *Options {
	this.PollInterval = duration
	return this
}

func (this *Options) WithHeartbeat(duration time.Duration) *Options {
	this.HeartbeatInterval = duration
	return this
}

func (this *Options) WithDeadline(duration time.Duration) *Options {
	this.Deadline = duration
	return this
}

func (this *Worker) generateMutateTransaction(id uuid.UUID) func(*sql.Tx) error {
	return func(tx *sql.Tx) error {
		// Delete from the queue in the transaction
		results, err := tx.Exec("DELETE FROM items WHERE id = $1 and claim = $2", id, this.uuid)
		if err != nil {
			return err
		}
		rowsAffected, err := results.RowsAffected()
		if err != nil {
			return err
		}
		if rowsAffected != 1 {
			return errors.New("Invalid")
		}
		return nil
	}
}

func (this *Worker) startWork() {
	payload := []byte{}
	id := uuid.UUID{}
	attempt, numRetries := 0, 0
	err := this.db.QueryRow("UPDATE items SET claim = $1 WHERE claim IS NULL ORDER BY added_at ASC LIMIT 1 RETURNING payload, id, attempt, num_retries", this.uuid).Scan(&payload, &id, &attempt, &numRetries)
	if err != nil {
		return
	}

	callbackError := this.callback(context.Background(), payload, this.generateMutateTransaction(id))
	// Non-retryable error or we have reached max number of retries; remove from the queue.
	if callbackError != nil && (!callbackError.Retryable() || numRetries-1 == attempt) {
		_, err = this.db.Exec("DELETE FROM items WHERE id = $1 and claim = $2", id, this.uuid)
	}
	if callbackError != nil && callbackError.Retryable() {
		_, err = this.db.Exec("UPDATE items SET (claim, added_at, attempt) = (NULL, now(), $1) WHERE id = $2 and claim = $3", attempt+1, id, this.uuid)
	}
}

func (this *Worker) doWork() {
	defer this.workerWaitGroup.Done()

	for {
		select {
		case <-this.workerDone:
			return
		case <-this.workPollTicker.C:
			this.startWork()
		}
	}
}

func (this *Worker) heartbeat() {
	// First, heartbeat this thread
	this.db.Exec("UPDATE sessions SET heartbeated_at = now() WHERE id = $1", this.uuid)

	// Delete expired sessions
	this.db.Exec("DELETE FROM sessions WHERE (now() - heartbeated_at) > $1", this.options.Deadline.Seconds())
}

func (this *Worker) cleanUpSession() {
	this.db.Exec("DELETE FROM sessions WHERE id = $1", this.uuid)
}

func (this *Worker) doHeartbeat() {
	defer this.heartbeatWaitGroup.Done()

	for {
		select {
		case <-this.heartbeatDone:
			this.cleanUpSession()
			return
		case <-this.heartbeatTicker.C:
			this.heartbeat()
		}
	}
}

func New(callback func(context.Context, []byte, func(*sql.Tx) error) Error, db *sql.DB, options *Options) (*Worker, error) {
	// Create a session

	// Create a session, grab a UUID
	worker := &Worker{
		callback:        callback,
		options:         options,
		workerDone:      make(chan bool),
		heartbeatDone:   make(chan bool),
		heartbeatTicker: time.NewTicker(options.HeartbeatInterval),
		workPollTicker:  time.NewTicker(options.PollInterval),
		db:              db,
	}

	// Grab  a new session for this worker
	err := db.QueryRow("INSERT INTO sessions (heartbeated_at) VALUES (now()) RETURNING id").Scan(&worker.uuid)
	if err != nil {
		return nil, err
	}

	// start 2 fibers
	worker.workerWaitGroup.Add(1)
	worker.heartbeatWaitGroup.Add(1)

	// Start a heartbeat fiber
	go worker.doWork()

	// Start a fiber that will handle work
	go worker.doHeartbeat()

	return worker, nil
}

func (this *Worker) Stop() {
	// Stop the worker thread, wait for it to exit
	this.workPollTicker.Stop()
	this.workerDone <- true
	this.workerWaitGroup.Wait()

	// Stop the heartbeat thread, wait for it to exit
	this.heartbeatTicker.Stop()
	this.heartbeatDone <- true
	this.heartbeatWaitGroup.Wait()
}
