#!/bin/bash
# Start the pglite server in the background, log its output, and save its PID.

LOG_FILE=/tmp/pglite.log
PID_FILE=/tmp/pglite.pid

# Ensure the bun binary path is available
export PATH="$HOME/.bun/bin:$PATH"

# Start the server in the background
nohup pglite-server -d ./pgdata >"$LOG_FILE" 2>&1 &

# Save the PID of the last background process
_pid=$!
echo $_pid > "$PID_FILE"

echo "pglite-server started with PID $_pid. Logs are at $LOG_FILE"
