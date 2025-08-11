# Makefile for ShopStream Data Pipeline
#
# This Makefile is used to install the Task tool, which is a modern build tool
# and task runner. It simplifies running development, testing, and deployment tasks.
#
# For more information on Task, see the official documentation:
# https://taskfile.dev

# Phony targets are not actual files, but commands to be executed.
.PHONY: all install-task help

# The default target, which shows the help message.
all: help

# Installs Task (https://taskfile.dev) to the local bin directory.
# This allows us to use Task for all subsequent operations.
install-task:
	if ! command -v task &> /dev/null; then \
		echo "Task not found. Installing..."; \
		sh -c "$(curl -sL https://taskfile.dev/install.sh)" -- -d -b ./bin; \
		echo "Task installed successfully in ./bin directory."; \
	else \
		echo "Task is already installed."; \
	fi

# Displays a help message listing all available tasks from the Taskfile.
# This serves as a quick reference for developers.
help:
	@echo "Please use 'task --list' to see all available tasks."
	@echo "First, ensure Task is installed by running: make install-task"
	@echo "Then, you can use './bin/task <task_name>' to run tasks."

