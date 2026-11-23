#!/bin/bash

# Reproduces a scenario where after a burst that causes the executor queue to overload,
# a datashard enters a pathological state where almost all CPU time gets wasted to reply
# OVERLOADED to read requests and very little actual work (that could help get out of the
# overloaded state) gets done, even after the burst subsides and the load returns to normal.

./ydb/tools/datashard_read_load/datashard_read_load run \
    --endpoint localhost:2135 \
    --database /local/testdb --table /local/testdb/mailbox_load \
    --threads 4 \
    --long-inflight 60000 --short-inflight 4096 --probe-inflight 16 \
    --dispatch-batch 256 \
    --phase sustained:40s:8000:0 \
    --phase burst:40s:32000:0 \
    --phase sustained_after:60s:8000:0 \
> overloaded.jsonl
