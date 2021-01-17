package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	melonade "github.com/devit-tel/melonade-client-go"
	"github.com/go-redis/redis/v8"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	var redisAddr = flag.String("redis", "localhost:6379", "redis addr")
	var ns = flag.String("ns", "staging", "namespace")
	var tn = flag.String("tn", "tms_create_trip", "task name to be ended")
	var melonadeUrl = flag.String("melonade", "http://localhost:8081", "melonade process manager url")
	flag.Parse()

	ctx := context.Background()
	rdbT := redis.NewClient(&redis.Options{
		Addr:     *redisAddr,
		Password: "", // no password set
		DB:       5,  // use default DB
	})

	rdbW := redis.NewClient(&redis.Options{
		Addr:     *redisAddr,
		Password: "", // no password set
		DB:       4,  // use default DB
	})

	ks, err := rdbT.Keys(ctx, fmt.Sprintf("melonade.%s.task.*", *ns)).Result()
	if err != nil {
		log.Fatal(err)
	}

	for _, k := range ks {
		vt, err := rdbT.Get(ctx, k).Result()
		if err != nil {
			log.Println("cannot get key", k, err)
			continue
		}

		var t melonade.Task
		err = json.Unmarshal([]byte(vt), &t)
		if err != nil {
			log.Fatal(err)
		}

		if t.TaskName == *tn && t.Status == melonade.TaskStatusScheduled {
			fmt.Println(t.TransactionID, t.WorkflowID, t.TaskID, time.Unix(0, t.StartTime*int64(time.Millisecond)).Format(time.RFC3339))
			fmt.Println("Press 'Y' to continue, n to skip")
			b, _ := bufio.NewReader(os.Stdin).ReadByte()
			if !(b == 'y' || b == 'Y') {
				fmt.Println("Skipped")
				continue
			}

			wk := fmt.Sprintf("melonade.%s.workflow.%s", *ns, t.WorkflowID)
			vw, err := rdbW.Get(ctx, wk).Result()
			if err != nil {
				log.Fatal(err)
			}

			var w map[string]interface{}
			err = json.Unmarshal([]byte(vw), &w)
			if err != nil {
				log.Fatal(err)
			}

			// Set retry to 0, so workflow won't retry after set task to FAILED!
			w["retries"] = 0

			bw, err := json.Marshal(w)
			if err != nil {
				log.Fatal(err)
			}

			_, err = rdbW.Set(ctx, wk, bw, 0).Result()
			if err != nil {
				log.Fatal(err)
			}

			tr := melonade.NewTaskResult(&t)
			tr.Status = melonade.TaskStatusFailed
			tr.DoNotRetry = true
			tr.IsSystem = true

			trs, err := json.Marshal(tr)
			if err != nil {
				log.Fatal(err)
			}

			res, err := http.Post(fmt.Sprintf("%s/v1/transaction/update", *melonadeUrl), "application/json", bytes.NewBuffer(trs))
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(res.Status)
			fmt.Printf("%s changed to FAILED\n", tr.TaskID)
		}

		//os.Exit(1)
	}
}
