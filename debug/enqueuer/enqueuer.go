package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"

	"github.com/jonathanmeisel/queue/enqueue"
	_ "github.com/lib/pq"
)

var dbConnectionString string
var payload string

func init() {
	flag.StringVar(&dbConnectionString, "db_connection_string", "postgresql://maxroach@localhost:26257/tokenindex?ssl=true&sslmode=require&sslrootcert=/home/jonathanmeisel/database_certs/certs/ca.cert&sslkey=/home/jonathanmeisel/database_certs/certs/client.maxroach.key&sslcert=/home/jonathanmeisel/database_certs/certs/client.maxroach.cert&password=abcd", "Database Connection String")
	flag.StringVar(&payload, "payload", "", "")
}

func main() {
	flag.Parse()

	db, err := sql.Open("postgres", dbConnectionString)
	if err != nil {
		log.Fatal("error connecting to the database: ", err)
	}

	enqueuer := enqueue.New(db, enqueue.NewOptions())
	err = enqueuer.Enqueue([]byte(payload))
	if err != nil {
		fmt.Println(err)
	}
}
