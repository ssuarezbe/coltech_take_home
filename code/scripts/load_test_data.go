package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/redis/go-redis/v9"
)

func main() {
	// The Go program is now run from the `code/scripts/` directory.
	// The data file is located at `../../Data/clickstream_events.json` relative to that.
	dataPath := filepath.Join("..", "..", "Data", "clickstream_events.json")

	// Read the entire file
	fileBytes, err := os.ReadFile(dataPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Test data file not found: %s\n", err)
		os.Exit(1)
	}

	// Unmarshal the JSON into a slice of generic maps
	var events []map[string]interface{}
	if err := json.Unmarshal(fileBytes, &events); err != nil {
		fmt.Fprintf(os.Stderr, "Invalid JSON in %s: %s\n", dataPath, err)
		os.Exit(1)
	}

	// Connect to Redis
	// Assumes Redis is running and accessible at the default address.
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	_, err = rdb.Ping(ctx).Result()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to Redis: %s\n", err)
		os.Exit(1)
	}

	sent := 0
	for _, ev := range events {
		userID, ok := ev["user_id"]
		if !ok {
			// Skip events without a user_id as the executor requires it
			continue
		}

		// Marshal the event map back to a JSON string
		payloadBytes, err := json.Marshal(ev)
		if err != nil {
			// This should not happen if the original JSON was valid
			continue
		}

		// Use XADD to add the event to the stream
		// The values are a map of field-value pairs.
		args := &redis.XAddArgs{
			Stream: "events-stream",
			Values: map[string]interface{}{
				"user_id": fmt.Sprintf("%v", userID),
				"payload": string(payloadBytes),
			},
		}

		if err := rdb.XAdd(ctx, args).Err(); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to send event: %s\n", err)
		} else {
			sent++
		}
	}

	fmt.Printf("Loaded %d events into 'events-stream'.\n", sent)
}
