#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
cd "$repo_root"

mapfile -t examples < <(
  cargo metadata --no-deps --format-version 1 \
    | jq -r '.packages[]
      | select(.name == "temporalio-sdk")
      | .targets[]
      | select(.kind[] == "example")
      | .name'
)

has_example() {
  local expected="$1"
  local example
  for example in "${examples[@]}"; do
    if [[ "$example" == "$expected" ]]; then
      return 0
    fi
  done
  return 1
}

worker_pid=""

cleanup_worker() {
  if [[ -n "$worker_pid" ]]; then
    kill "$worker_pid" 2>/dev/null || true
    wait "$worker_pid" 2>/dev/null || true
    worker_pid=""
  fi
}

trap cleanup_worker EXIT

for starter in "${examples[@]}"; do
  if [[ "$starter" != *-starter ]]; then
    continue
  fi

  sample="${starter%-starter}"
  worker="${sample}-worker"

  if ! has_example "$worker"; then
    echo "Skipping $starter because $worker is not declared"
    continue
  fi

  cargo run -p temporalio-sdk --features examples --example "$worker" &
  worker_pid=$!

  set +e
  timeout 20 cargo run -p temporalio-sdk --features examples --example "$starter"
  starter_status=$?
  set -e

  cleanup_worker

  if [[ "$starter_status" -ne 0 ]]; then
    exit "$starter_status"
  fi
done
