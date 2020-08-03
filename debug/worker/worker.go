package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/jonathanmeisel/queue/worker"
)

var dbConnectionString string

func init() {
	flag.StringVar(&dbConnectionString, "db_connection_string", "postgresql://maxroach@localhost:26257/tokenindex?ssl=true&sslmode=require&sslrootcert=/home/jonathanmeisel/database_certs/certs/ca.cert&sslkey=/home/jonathanmeisel/database_certs/certs/client.maxroach.key&sslcert=/home/jonathanmeisel/database_certs/certs/client.maxroach.cert&password=abcd", "Database Connection String")
}

var quit = make(chan struct{})

func main() {
	flag.Parse()

	db, err := sql.Open("postgres", dbConnectionString)
	if err != nil {
		log.Fatal("error connecting to the database: ", err)
	}

	_, err = worker.New(func(_ context.Context, payload []byte, updateTransaction func(*sql.Tx, error) error) {
		fmt.Println(string(payload))
		err = crdb.ExecuteTx(context.Background(), db, nil, func(tx *sql.Tx) error {
			updateTransaction(tx, nil)
			return nil
		})

	}, db, worker.NewOptions())
	<-quit
}
