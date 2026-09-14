#!/bin/bash
# DDL churn against /Root: creates partitioned tables and drops older ones.
# Exercises SchemeShard -> Hive -> BSController, the chain whose cross-tablet
# references the consistency checker compares.
E=grpc://localhost:2135
D=/Root
LOG=~/workload.log
STATE=~/workload_live.txt

: > "$STATE"
i=0
while true; do
  i=$((i + 1))
  name="w_$(date +%s)_$i"
  if timeout 30 ydb -e $E -d $D sql -s "CREATE TABLE $name (id Uint64 NOT NULL, payload String, PRIMARY KEY(id)) WITH (AUTO_PARTITIONING_BY_SIZE = DISABLED, UNIFORM_PARTITIONS = 2);" >/dev/null 2>&1; then
    echo "$(date -u +%FT%TZ) create ok $name" >> "$LOG"
    echo "$name" >> "$STATE"
  else
    echo "$(date -u +%FT%TZ) create FAIL $name" >> "$LOG"
  fi

  # Keep a rolling window: drop the oldest once more than 10 are alive.
  count=$(wc -l < "$STATE")
  if [ "$count" -gt 10 ]; then
    victim=$(head -1 "$STATE")
    if timeout 30 ydb -e $E -d $D sql -s "DROP TABLE $victim;" >/dev/null 2>&1; then
      echo "$(date -u +%FT%TZ) drop ok $victim" >> "$LOG"
      sed -i 1d "$STATE"
    else
      echo "$(date -u +%FT%TZ) drop FAIL $victim" >> "$LOG"
    fi
  fi
  sleep 2
done
