# How to Run the Streaming Analytics System

This document provides instructions for running the Redis, pglite, and Go executor components of the system. This project uses `Task` as a command runner. Please ensure you have installed all dependencies by running `task install` first.

## 1. Prepare the Environment

Before starting the main components, you need to start the Redis container and load the test data into the stream. This can be done with a single command:

```bash
task load-test-data
```

This command ensures Redis is running and populates the `events-stream` with test data.

## 2. Run the Services

The `pglite` database and the Go executor must be run in separate terminals to see their log output and manage them effectively.

### Terminal 1: Start the pglite Server

Open a new terminal and run the following command. This will start the `pglite` database server in the foreground. This terminal will be dedicated to the database process.

```bash
task run-pglite-fg
```

You should see output indicating that the server is listening on port `5432`.

### Terminal 2: Start the Go Executor

Open a second terminal. In this terminal, run the command to start the Go executor. It will automatically connect to Redis and pglite, and begin processing events from the stream.

```bash
task run-executor-fg
```

You will see the executor's log output in this terminal as it processes events.


## Terminal 3: Check the data

You can check the data in the pglite database by running the following command:

```bash
% task pg-cli
task: [pg-cli] psql "postgresql://user:password@localhost:5432/postgres?sslmode=disable"
psql (14.18 (Homebrew), server 0.0.0)
Type "help" for help.

postgres=> SELECT tablename FROM pg_tables WHERE schemaname = 'public';
 tablename 
-----------
 events
(1 row)

postgres=> select * from events;
 id |    event_id     |  user_id   |                                                                                                                                                                                                                            payload                                                                                                                                                                                                                             |        created_at         
----+-----------------+------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------
  1 | 1754688025111-0 | usr_ghi789 | {"country": "UK", "user_id": "usr_ghi789", "event_id": "evt_1234567895", "page_url": "/products/phone-789", "referrer": "instagram.com", "timestamp": "2024-01-15T11:00:00.000Z", "event_type": "page_view", "ip_address": "172.16.0.100", "properties": {"price": 1499.99, "category": "electronics", "product_id": "prod_phone_789"}, "session_id": "ses_bbb222", "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)", "device_type": "desktop"} | 2025-08-08 21:22:41.25+00
(1 row)

postgres=> \q
```

## Terminal 4: Stopping the Services

To stop the services, you can use `Ctrl+C` in each of the terminals. To stop the Redis container, you can run:

```bash
task stop-services
```
