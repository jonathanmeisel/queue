package enqueue

import "database/sql"

type Enqueuer struct {
	db      *sql.DB
	options *Options
}

type Options struct {
	numRetries int
}

func NewOptions() *Options {
	return &Options{
		numRetries: 3,
	}
}

func (this *Options) WithNumRetries(numRetries int) *Options {
	this.numRetries = numRetries
	return this
}

func New(db *sql.DB, options *Options) *Enqueuer {
	return &Enqueuer{
		db:      db,
		options: options,
	}
}

func (this *Enqueuer) Enqueue(payload []byte) error {
	_, err := this.db.Exec("INSERT into items (added_at, payload, attempt, num_retries) VALUES (now(), $1, 0, $2)", payload, this.options.numRetries)
	return err
}
