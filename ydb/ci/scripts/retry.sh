#!/usr/bin/env bash

usage() {
  echo "$0 -r [max_attempts] -t [timeout] -s [sleep] -- cmd"
}
if [ $# -lt 2 ]; then
  usage
  exit 1
fi

max_attempts=0
timeout=1800
sleep_time=30

while getopts 'r:t:s:' opt; do
  case "$opt" in
    r)
      max_attempts=$OPTARG
      ;;
    t)
      timeout=$OPTARG
      ;;
    s)
      sleep_time=$OPTARG
      ;;
    *)
      usage
      exit 1
      ;;
  esac
done


shift $((OPTIND - 2))

if [[ "$1" == "--" ]]; then
  shift  # Shift past the double dash
else
  echo "Error: Missing -- before command"
  exit 1
fi

attempt_num=1
start_time=$(date +%s)

while true; do
  elapsed=$(($(date +%s) - start_time))

  if [ "$max_attempts" -ne 0 ] && [ "$attempt_num" -ge "$max_attempts" ]; then
    echo "maximum attempts reached, exit" >&2
    exit 10
  fi

  if [ "$timeout" -ne 0 ] && [ $elapsed -ge "$timeout" ]; then
    echo "timeout reached, exit" >&2
    exit 11
  fi

  if "$@"; then
    exit
  else
    attempt_num=$(( attempt_num + 1 ))
    if [ "$sleep_time" != "0" ]; then
      sleep "$sleep_time"
    fi
  fi
done