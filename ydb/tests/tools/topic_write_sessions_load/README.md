# Write Session Load

Opens YDB Topic write sessions using the C++ SDK. No messages are written.
Use only against a disposable test cluster: this tool intentionally retains
sessions and can exhaust memory on both the cluster and the load generator.

From the repository root:

```bash
export YDB_TOKEN='...'
bash ydb/tests/tools/topic_write_sessions_load/run.sh 1000 \
  --endpoint grpcs://cluster.example:2135 \
  --database /Root/test \
  --topic /Root/test/load-topic \
  --partition 0 \
  --duration 120 \
  --hold 120 \
  --max-sessions 100000
```

The positional argument is the target number of new SDK sessions per second.
The topic must already exist. The script builds the binary with `ya make` and
runs it. Subsequent runs use the build cache; the binary can also run directly.
Authentication uses `YDB_TOKEN`, or anonymous access if the variable is unset.
Use `--ca-file cert.pem` for a private TLS CA. Discovered node endpoints must be
reachable from the load generator.

By default all sessions target the specified partition with
`DirectWriteToPartition(true)`. The SDK resolves its location and generation.
Each session has a unique producer ID. There are no automatic session retries,
so reconnect storms do not silently inflate the requested rate. Use
`--normal-write` to compare the non-direct path, which bypasses this quoter.

Creation stops at `--duration` or `--max-sessions`, whichever comes first.
Existing and initializing sessions remain alive for `--hold` additional seconds.
Ctrl+C stops creation and closes retained sessions immediately. The default SDK
connection timeout is overridden to 300 seconds; adjust `--connect-timeout` for
long queueing experiments. Failed/closed sessions are released each reporting
interval and are not replaced beyond the configured creation rate.

## Statistics

- `started_rps`: actual calls to `CreateWriteSession` per second, not server RPS.
- `initialized_rps`: completed server handshakes per second (`GetInitSeqNo`).
- `pending`: SDK sessions still awaiting initialization, including discovery and
  transport waits. This is not the server quoter's queue length.
- `retained`: sessions not yet observed closed, including pending sessions.
- `init_failed`, `closed`, `status_*`: cumulative failures and close statuses
  before intentional shutdown; `status_*` uses numeric SDK status codes.
- `avg_init_ms`: cumulative average initialization latency for successful sessions.
- `skipped_slots`: launches skipped when the generator cannot sustain the target
  rate. Missed launches are not emitted as a catch-up burst.

Run the same command against builds before and after the fix, using the same
partition count, RPS, duration and server configuration. Start with a modest rate
and increase it. Compare server memory and actor counts for `FRONT_PQ_WRITE`,
`PQ_PARTITION_CHOOSER`, `KQP_SESSION_ACTOR` and `KQP_PROXY_ACTOR`. With the fix,
initialization throughput should be limited while pending sessions accumulate.
This is not a bound on the memory of all open sessions: the client and
`TWriteSessionActor` still consume memory. Also watch the generator's RSS, CPU,
file descriptors, network and gRPC stream limits; a client bottleneck can mask
server behavior.

Exit codes: 0 for completion without observed errors, 1 for session failures or
premature closes, 2 for setup/argument errors, 130 for interruption. Completion
does not require all pending sessions to initialize before the hold period ends.
