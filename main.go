package main

import (
	"context"
	"encoding/json"
	"github.com/go-redis/redis/v8"
	"log"
	"os"
)

func main() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       5,  // use default DB
	})

	ks, err := rdb.Keys(ctx, "*").Result()
	if err != nil {
		log.Fatal(err)
	}

	for _, k := range ks {
		v, err := rdb.Get(ctx, k).Result()
		if err != nil {
			log.Fatal(err)
		}

		t := make(map[string]interface{})
		err = json.Unmarshal([]byte(v), &t)
		if err != nil {
			log.Fatal(err)
		}

		log.Println(t["taskId"])

		os.Exit(1)
	}
}
