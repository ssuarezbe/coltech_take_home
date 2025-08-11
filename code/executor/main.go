package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
)

// Executor holds the dependencies for the stream processor.
// It includes clients for Redis and PostgreSQL.
type Executor struct {
	redisClient *redis.Client
	pgClient    *sql.DB
}

// NewExecutor creates a new Executor with connections to Redis and PostgreSQL.
const (
	streamName       = "events-stream"
	dlqStreamName     = "dlq"
	groupName        = "processing-group"
	sessionTTL       = 30 * time.Minute
	anomalyWindow    = 1 * time.Minute
	anomalyThreshold = 100
)

// NewExecutor creates a new Executor with connections to Redis and PostgreSQL.
func NewExecutor(redisAddr, pgConnStr string) (*Executor, error) {
	// Initialize Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	// Ping Redis to ensure the connection is alive.
	if _, err := redisClient.Ping(context.Background()).Result(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	// Initialize PostgreSQL client
	pgClient, err := sql.Open("postgres", pgConnStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	// Configure the connection pool to prevent timeouts.
	pgClient.SetConnMaxLifetime(time.Minute) // Connections are recycled after 1 minute.
	pgClient.SetMaxOpenConns(10)
	pgClient.SetMaxIdleConns(5)

	// Ping PostgreSQL to ensure the connection is alive.
	if err := pgClient.Ping(); err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	exec := &Executor{
		redisClient: redisClient,
		pgClient:    pgClient,
	}

	// Setup the database schema
	if err := exec.setupSchema(); err != nil {
		return nil, fmt.Errorf("failed to setup database schema: %w", err)
	}

	return exec, nil
}

// setupSchema creates the necessary tables in the PostgreSQL database if they don't exist.
func (e *Executor) setupSchema() error {
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS events (
		id SERIAL PRIMARY KEY,
		event_id VARCHAR(255) NOT NULL,
		user_id VARCHAR(255) NOT NULL,
		payload JSONB,
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);`

	_, err := e.pgClient.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create events table: %w", err)
	}
	log.Println("Database schema is ready.")
	return nil
}

// ProcessEvent handles a single event from the stream.
// It implements session management, anomaly detection, and data storage.
func (e *Executor) ProcessEvent(ctx context.Context, message redis.XMessage) error {
	log.Printf("Processing event ID: %s, Data: %v\n", message.ID, message.Values)

	userID, ok := message.Values["user_id"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid user_id in event %s", message.ID)
	}

	// 1. Session Management
	sessionKey := fmt.Sprintf("session:%s", userID)
	if err := e.redisClient.Set(ctx, sessionKey, "active", sessionTTL).Err(); err != nil {
		return fmt.Errorf("failed to update session for user %s: %w", userID, err)
	}

	// 2. Anomaly Detection
	anomalyKey := fmt.Sprintf("events:user:%s", userID)
	now := time.Now()
	// Add current event with timestamp as score
	e.redisClient.ZAdd(ctx, anomalyKey, &redis.Z{Score: float64(now.UnixNano()), Member: message.ID})
	// Remove events older than the window
	e.redisClient.ZRemRangeByScore(ctx, anomalyKey, "-inf", fmt.Sprintf("%d", now.Add(-anomalyWindow).UnixNano()))
	// Get current event count
	count, err := e.redisClient.ZCard(ctx, anomalyKey).Result()
	if err != nil {
		return fmt.Errorf("failed to get event count for user %s: %w", userID, err)
	}

	if count > anomalyThreshold {
		log.Printf("[ALERT] High frequency of events for user %s: %d events in the last minute", userID, count)
	}

	// 3. Data Output to PostgreSQL
	payload, _ := message.Values["payload"].(string) // Assuming payload is a JSON string
	_, err = e.pgClient.ExecContext(ctx, "INSERT INTO events (event_id, user_id, payload) VALUES ($1, $2, $3)", message.ID, userID, payload)
	if err != nil {
		return fmt.Errorf("failed to insert event %s into PostgreSQL: %w", message.ID, err)
	}

	return nil
}

// ConsumeEvents starts consuming events from the Redis Stream using a consumer group.
// It runs in a loop until the context is canceled.
func (e *Executor) ConsumeEvents(ctx context.Context, streamName, groupName, consumerName string) {
	// Ensure the consumer group exists. If not, create it.
	// '0-0' means start from the beginning of the stream.
	// MkStream ensures the stream is created if it doesn't exist.
	if _, err := e.redisClient.XGroupCreateMkStream(ctx, streamName, groupName, "0-0").Result(); err != nil {
		if err.Error() != "BUSYGROUP Consumer Group name already exists" {
			log.Printf("Error creating consumer group: %v", err)
		}
	}

	log.Printf("Starting to consume events from stream '%s' with group '%s'\n", streamName, groupName)

	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping event consumer.")
			return
		default:
			// Read from the stream. Block for 2 seconds if no new messages.
			streams, err := e.redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    groupName,
				Consumer: consumerName,
				Streams:  []string{streamName, ">"}, // '>' means get new messages
				Count:    10,                      // Process up to 10 messages at a time
				Block:    2 * time.Second,
			}).Result()

			if err != nil && err != redis.Nil {
				log.Printf("Error reading from stream: %v", err)
				continue
			}

			for _, stream := range streams {
				for _, message := range stream.Messages {
					go func(msg redis.XMessage) {
						if err := e.ProcessEvent(ctx, msg); err != nil {
							log.Printf("Error processing event %s: %v. Moving to DLQ.", msg.ID, err)
							// Move to DLQ on failure
							e.redisClient.XAdd(ctx, &redis.XAddArgs{
								Stream: dlqStreamName,
								Values: msg.Values,
							})
						} 
						// Acknowledge the message so it's not re-delivered from the main stream.
						e.redisClient.XAck(ctx, streamName, groupName, msg.ID)
					}(message)
				}
			}
		}
	}
}

func main() {
	log.Println("Starting ShopStream Executor...")

	// Configuration - ideally from environment variables or a config file.
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	pgConnStr := os.Getenv("PG_CONN_STR")
	if pgConnStr == "" {
		pgConnStr = "postgresql://user:password@localhost:5432/postgres?sslmode=disable"
	}
	consumerName, _ := os.Hostname()
	log.Printf("Configuration: REDIS_ADDR=%s, PG_CONN_STR=%s, ConsumerName=%s", redisAddr, pgConnStr, consumerName)

	// Create a new executor.
	executor, err := NewExecutor(redisAddr, pgConnStr)
	if err != nil {
		log.Fatalf("Failed to initialize executor: %v", err)
	}

	// Create a context that we can cancel on shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start consuming events in a separate goroutine.
	go executor.ConsumeEvents(ctx, streamName, groupName, consumerName)

	// Wait for a shutdown signal.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down executor...")
}
