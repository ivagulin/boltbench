package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/olekukonko/tablewriter"
	"github.com/samber/lo"
	"log"
	"log/slog"
	"math/rand/v2"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var (
	concurrency = flag.Int("concurrency", 24, "Number of concurrent goroutines")
	benchtime   = flag.Duration("benchtime", 60*time.Second, "Bench time")
	scale       = flag.Int("scale", 1000, "Scaling factor")
	RWMode      = flag.Bool("rwmode", true, "Read write mode")
	initMode    = flag.Bool("init", true, "init")
)

var (
	accountPrefix = []byte("accounts:")
	tellerPrefix  = []byte("tellers:")
	branchPrefix  = []byte("branches:")
	historyPrefix = []byte("history:")
)

type Account struct {
	AID      int    `db:"aid"`
	BID      int64  `db:"bid"`
	Abalance int64  `db:"abalance"`
	Filler   string `db:"filler"`
}

type Teller struct {
	TID      int    `db:"tid"`
	BID      int64  `db:"bid"`
	Tbalance int64  `db:"tbalance"`
	Filler   string `db:"filler"`
}

type Branche struct {
	BID      int    `db:"bid"`
	Bbalance int64  `db:"bbalance"`
	Filler   string `db:"filler"`
}

type History struct {
	TID    int64     `db:"tid"`
	BID    int64     `db:"bid"`
	AID    int64     `db:"aid"`
	Delta  int64     `db:"delta"`
	Mtime  time.Time `db:"mtime"`
	Filler string    `db:"filler"`
}

func keyFor(id int) []byte {
	return []byte(strconv.Itoa(id))
}

func valueFor(val interface{}) []byte {
	rv, err := json.Marshal(val)
	if err != nil {
		panic(err)
	}
	return rv
}

func fillTable(db *bolt.DB, prefix []byte, limit int, genfunc func(it int) interface{}) {
	created := 0
	err := db.View(func(txn *bolt.Tx) error {
		b := txn.Bucket(prefix)
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			created++
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	for created < limit {
		slog.Info("filling table", "prefix", prefix, "limit", limit, "created", created)
		err = db.Update(func(txn *bolt.Tx) error {
			b := txn.Bucket(prefix)
			for it := created; it < limit && it-created < 1000; it++ {
				val := genfunc(it)
				b.Put(keyFor(it), valueFor(val))
			}
			created += 1000
			return nil
		})
		if err != nil {
			panic(err)
		}
	}
}

func fill(db *bolt.DB) {
	accountsToCreate := *scale * 100_000
	tellersToCreate := *scale * 10
	branchesToCreate := *scale * 1

	fillTable(db, accountPrefix, accountsToCreate, func(it int) interface{} {
		return Account{AID: it}
	})

	fillTable(db, tellerPrefix, tellersToCreate, func(it int) interface{} {
		return Teller{TID: it}
	})

	fillTable(db, branchPrefix, branchesToCreate, func(it int) interface{} {
		return Branche{BID: it}
	})
}

func readWrite(db *bolt.DB) {
	aid := rand.IntN(*scale * 100_000)
	tid := rand.IntN(*scale * 10)
	bid := rand.IntN(*scale * 1)
	adelta := rand.Int64N(10000) - 5000
	err := db.Update(func(txn *bolt.Tx) error {
		//SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
		accBucket := txn.Bucket(accountPrefix)
		accVal := accBucket.Get(keyFor(aid))
		if accVal == nil {
			panic("account not found for key")
		}
		var acc Account
		lo.Must0(json.Unmarshal(accVal, &acc))

		//UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;
		acc.Abalance += adelta
		accBucket.Put(keyFor(aid), valueFor(acc))

		//UPDATE pgbench_tellers SET tbalance = tbalance + :delta WHERE tid = :tid;
		tellerBucket := txn.Bucket(tellerPrefix)
		tellerVal := tellerBucket.Get(keyFor(tid))
		if tellerVal == nil {
			panic("teller not found for key")
		}
		var teller Teller
		lo.Must0(json.Unmarshal(tellerVal, &teller))
		teller.Tbalance += adelta
		tellerBucket.Put(keyFor(tid), valueFor(teller))

		//UPDATE pgbench_branches SET bbalance = bbalance + :delta WHERE bid = :bid;
		branchBucket := txn.Bucket(branchPrefix)
		branchVal := branchBucket.Get(keyFor(bid))
		if branchVal == nil {
			panic("branch not found for key")
		}
		var branch Branche
		lo.Must0(json.Unmarshal(branchVal, &branch))
		branch.Bbalance += adelta
		branchBucket.Put(keyFor(tid), valueFor(branch))

		//INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);
		historyBucket := txn.Bucket(historyPrefix)
		historyBucket.Put(keyFor(int(lo.Must(historyBucket.NextSequence()))), valueFor(History{
			AID:   int64(aid),
			TID:   int64(tid),
			BID:   int64(bid),
			Delta: adelta,
			Mtime: time.Now(),
		}))
		return nil
	})
	if err != nil {
		panic(err)
	}
}

func read(db *bolt.DB) {
	aid := rand.IntN(*scale * 100_000)
	err := db.View(func(txn *bolt.Tx) error {
		//SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
		accBucket := txn.Bucket(accountPrefix)
		accVal := accBucket.Get(keyFor(aid))
		if accVal == nil {
			panic("account not found for key")
		}
		var acc Account
		lo.Must0(json.Unmarshal(accVal, &acc))
		return nil
	})
	if err != nil {
		panic(err)
	}
}

func main() {
	flag.Parse()

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	db, err := bolt.Open("my.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	db.Update(func(tx *bolt.Tx) error {
		for _, table := range [][]byte{accountPrefix, tellerPrefix, branchPrefix, historyPrefix} {
			lo.Must(tx.CreateBucketIfNotExists(table))
		}
		return nil
	})

	slog.Info("filling...", "scale", *scale)
	if *initMode {
		fill(db)
	}

	slog.Info("testing...")
	var iterations uint64
	var conflicts uint64
	finishTimer, cancelFunc := context.WithTimeout(context.Background(), *benchtime)
	defer cancelFunc()
	var wg sync.WaitGroup
	wg.Add(*concurrency)
	for _ = range *concurrency {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-finishTimer.Done():
					return
				default:
					atomic.AddUint64(&iterations, 1)
					if *RWMode {
						readWrite(db)
					} else {
						read(db)
					}
				}
			}
		}()
	}
	wg.Wait()

	slog.Info("throughtput results", "concurrency", *concurrency, "iterations", iterations, "conflicts", conflicts)
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Name", "Latency(us)", "Throughput(rps)"})
	table.SetBorder(false)
	table.SetHeaderLine(false)
	table.SetRowLine(false)
	testName := lo.Ternary(*RWMode, "tpcb-like", "tpcb-readonly")
	latency := fmt.Sprintf("%0.3f", float64(benchtime.Microseconds())/float64(iterations)*float64(*concurrency))
	throughput := fmt.Sprintf("%0.3f", float64(iterations)/benchtime.Seconds())
	table.Append([]string{testName, latency, throughput})
	table.Render()

}
