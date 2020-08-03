package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jonathanmeisel/queue/worker"
)

func main() {
	worker, err := worker.New(func(_ context.Context, payload []byte, updateTransaction func(*sql.Tx, error) error) {
		fmt.Println("callback")
	}, /* database */ worker.NewOptions())

	time.Sleep(10 * time.Second)
	worker.Stop()
}
