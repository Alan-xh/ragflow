#!/bin/bash

# 遇到命令非零状态退出立即停止执行
set -e

# 加载当前 shell 文件路径的环境变量
load_env_file() {
    # Get the directory of the current script
    local script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local env_file="$script_dir/.env"

    # Check if .env file exists
    if [ -f "$env_file" ]; then
        echo "Loading environment variables from: $env_file"
        # Source the .env file
        set -a  # 之后定义的变量为环境变量
        source "$env_file" 
        set +a # 关闭之后定义的变量为环境变量，以防篡改 .env 中定义的环境变量
    else
        echo "Warning: .env file not found at: $env_file"
    fi
}

# Load environment variables
load_env_file

# Unset HTTP proxies that might be set by Docker daemon
export http_proxy=""; export https_proxy=""; export no_proxy=""; export HTTP_PROXY=""; export HTTPS_PROXY=""; export NO_PROXY=""
export PYTHONPATH=$(pwd)

# 切换 jemalloc 作为默认内存分配器
export LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/
JEMALLOC_PATH=$(pkg-config --variable=libdir jemalloc)/libjemalloc.so

PY=python3

# Set default number of workers if WS is not set or less than 1
if [[ -z "$WS" || $WS -lt 1 ]]; then
  WS=1
fi

# Maximum number of retries for each task executor and server
MAX_RETRIES=5

# Flag to control termination
STOP=false

# Array to keep track of child PIDs
PIDS=()

# Set the path to the NLTK data directory
export NLTK_DATA="./nltk_data"

# 清平并且退出所有进程
cleanup() {
  echo "Termination signal received. Shutting down..."
  STOP=true
  # Terminate all child processes
  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      echo "Killing process $pid"
      kill "$pid"
    fi
  done
  exit 0
}

# Trap SIGINT and SIGTERM to invoke cleanup
trap cleanup SIGINT SIGTERM

# Function to execute task_executor with retry logic 重试任务执行器
task_exe(){
    local task_id=$1
    local retry_count=0
    while ! $STOP && [ $retry_count -lt $MAX_RETRIES ]; do
        echo "Starting task_executor.py for task $task_id (Attempt $((retry_count+1)))"
        LD_PRELOAD=$JEMALLOC_PATH $PY rag/svr/task_executor.py "$task_id"
        EXIT_CODE=$?    #  $? 特殊变量 获取上一个执行的命令或进程的退出状态
        if [ $EXIT_CODE -eq 0 ]; then
            echo "task_executor.py for task $task_id exited successfully."
            break
        else
            echo "task_executor.py for task $task_id failed with exit code $EXIT_CODE. Retrying..." >&2
            retry_count=$((retry_count + 1))
            sleep 2
        fi
    done

    if [ $retry_count -ge $MAX_RETRIES ]; then
        echo "task_executor.py for task $task_id failed after $MAX_RETRIES attempts. Exiting..." >&2
        cleanup
    fi
}

# Function to execute ragflow_server with retry logic
run_server(){
    local retry_count=0
    while ! $STOP && [ $retry_count -lt $MAX_RETRIES ]; do
        echo "Starting ragflow_server.py (Attempt $((retry_count+1)))"
        $PY api/ragflow_server.py
        EXIT_CODE=$?
        if [ $EXIT_CODE -eq 0 ]; then
            echo "ragflow_server.py exited successfully."
            break
        else
            echo "ragflow_server.py failed with exit code $EXIT_CODE. Retrying..." >&2
            retry_count=$((retry_count + 1))
            sleep 2
        fi
    done

    if [ $retry_count -ge $MAX_RETRIES ]; then
        echo "ragflow_server.py failed after $MAX_RETRIES attempts. Exiting..." >&2
        cleanup
    fi
}

# Start task executors  创建 WS 个任务执行器 i 表示 task id
for ((i=0;i<WS;i++))
do
  task_exe "$i" &
  PIDS+=($!)
done

# Start the main server 开启一个主服务
run_server &
PIDS+=($!)

# Wait for all background processes to finish
wait
