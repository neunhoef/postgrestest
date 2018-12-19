package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

// Database was setup in the following way:
//   createdb mydb
//   createuser honk --pwprompt
// and then password "abc" entered. Then:
//   psql mydb
// and in there:
//   GRANT ALL ON DATABASE mydb TO honk;
//   CREATE TABLE t (key varchar(20) UNIQUE, hallo int, s varchar(1536));
//   GRANT ALL ON t TO honk;

func makeRandomString() string {
	var b strings.Builder
	r := rand.Intn(20000000) - 10000000
	d := rand.Intn(10000)
	for i := 0; i <= 125; i++ {
		by := strconv.AppendInt(make([]byte, 0, 12), int64(r), 10)
		b.Write(by)
		r += d
	}
	return b.String()
}

func initDatabase(db *sql.DB) {
	result, err := db.ExecContext(context.Background(),
		"DROP TABLE t;")
	if err != nil {
		fmt.Printf("Error in DROP TABLE t: %v\n", err)
	} else {
		fmt.Printf("DROP TABLE t: result: %v\n", result)
	}
	result, err = db.ExecContext(context.Background(),
		"CREATE TABLE t (key varchar(20) UNIQUE, hallo int, s varchar(1536));")
	if err != nil {
		fmt.Printf("Error in CREATE TABLE t: %v\n", err)
	} else {
		fmt.Printf("CREATE TABLE t: result: %v\n", result)
	}
	result, err = db.ExecContext(context.Background(),
		"GRANT ALL ON t TO honk;")
	if err != nil {
		fmt.Printf("Error in GRANT: %v\n", err)
	} else {
		fmt.Printf("GRANT: result: %v\n", result)
	}
}

func writeRows(db *sql.DB, n int64) {
	var b strings.Builder
	b.WriteString("INSERT INTO t (key, hallo, s) VALUES\n")
	le := n / 10000
	times := make([]int64, le, le)
	var i int64
	for i = 0; i < n; i++ {
		b.WriteString(fmt.Sprintf("('%s', %d, '%s')\n",
			"K"+strconv.FormatInt(i, 10),
			int64(i),
			makeRandomString()))
		if i%10000 == 0 {
			b.WriteString(";")
			startTime := time.Now()
			_, err := db.ExecContext(context.Background(), b.String())
			endTime := time.Now()
			dur := endTime.Sub(startTime)
			times[i/10000] = int64(dur)
			if err != nil {
				fmt.Printf("Error from query: %v\n", err)
			} else {
				fmt.Printf("Time for insert 10000: %v\n", dur)
			}
			b.Reset()
			b.WriteString("INSERT INTO t (key, hallo, s) VALUES\n")
		} else {
			b.WriteString(",\n")
		}
	}
	sort.Slice(times, func(a, b int) bool { return times[a] < times[b] })
	fmt.Printf(`Median: %d
90%%ile: %d
99%%ile: %d
min   : %d
max   : %d
`, times[int(le/2)], times[int(float64(le)*0.9)], times[int(float64(le)*0.99)],
		times[0], times[le-1])
}

func writeRowsOverwrite(db *sql.DB, n int64) {
	var b strings.Builder
	b.WriteString("INSERT INTO t (key, hallo, s) VALUES\n")
	le := n / 10000
	times := make([]int64, le, le)
	var (
		i int64
		j int64
	)
	j = 0
	for i = 0; i < n; i++ {
		ss := makeRandomString()
		b.WriteString(fmt.Sprintf("('%s', %d, '%s')\n",
			"K"+strconv.FormatInt(j, 10), j, ss))
		if i%10000 == 0 {
			b.WriteString("ON CONFLICT (key) DO UPDATE SET hallo = EXCLUDED.hallo, s = EXCLUDED.s;")
			startTime := time.Now()
			_, err := db.ExecContext(context.Background(), b.String())
			endTime := time.Now()
			dur := endTime.Sub(startTime)
			times[i/10000] = int64(dur)
			if err != nil {
				fmt.Printf("Error from query: %v\n", err)
			} else {
				fmt.Printf("Time for insert 10000: %v\n", dur)
			}
			b.Reset()
			b.WriteString("INSERT INTO t (key, hallo, s) VALUES\n")
		} else {
			b.WriteString(",\n")
		}
		j = j + 99991 // a prime
		for j > n {
			j -= n
		}
	}
	sort.Slice(times, func(a, b int) bool { return times[a] < times[b] })
	fmt.Printf(`Median: %d
90%%ile: %d
99%%ile: %d
min   : %d
max   : %d
`, times[int(le/2)], times[int(float64(le)*0.9)], times[int(float64(le)*0.99)],
		times[0], times[le-1])
}

func showData(db *sql.DB) error {
	rows, err := db.Query("SELECT * FROM t")
	if err != nil {
		fmt.Printf("Query error: %v\n", err)
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			key   string
			hallo int64
			s     string
		)
		if err := rows.Scan(&key, &hallo, &s); err != nil {
			fmt.Printf("Query error: %v\n", err)
			return err
		}
		fmt.Printf("%s, %d, %s\n", key, hallo, s)
	}
	return nil
}

func printUsage() {
	fmt.Println(`Usage:
	postgrestest init         - to initialize the database
	postgrestest insert       - insert 10000000 rows
	postgrestest upsert       - upsert 10000000 rows
`)
}

func main() {
	rand.Seed(time.Now().Unix())
	connStr := "postgres://honk:abc@localhost/mydb"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		fmt.Printf("Connection error: %v\n", err)
		return
	}
	if len(os.Args) < 2 {
		printUsage()
		return
	}
	switch os.Args[1] {
	case "init":
		initDatabase(db)
	case "insert":
		writeRows(db, 10000000)
	case "upsert":
		writeRowsOverwrite(db, 10000000)
	default:
		printUsage()
	}
	// if err = showData(db); err != nil {
	//   fmt.Printf("Query error: %v\n", err)
	// }
	if err = db.Close(); err != nil {
		fmt.Printf("Close error: %v\n", err)
	}
}
