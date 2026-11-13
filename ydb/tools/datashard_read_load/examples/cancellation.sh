#!/bin/bash

# Reproduces a scenario described in https://github.com/ydb-platform/ydb/issues/14936
# If a queue of read requests forms in a datashard as a result of a load burst,
# and the cancellation of requests in the queue doesn't happen, the latency will remain
# high and almost all requests will continue to time out even after the burst subsides
# and the load returns to normal.

./ydb/tools/datashard_read_load/datashard_read_load run \
    --endpoint localhost:2135 \
    --database /local/testdb --table /local/testdb/mailbox_load \
    --threads 4 \
    --rows-per-read 256 \
    --long-inflight 0 --short-inflight 16384 --probe-inflight 16 \
    --cancel-after 200ms \
    --phase sustained:40s:0:8000 \
    --phase burst:20s:0:20000 \
    --phase sustained_after:60s:0:8000 \
    --phase recovery:20s:0:100 \
    > cancellation.jsonl
