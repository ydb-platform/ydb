# local-ydb healthcheck

Docker runs `/health_check`; the entrypoint runs `/health_check --readiness`
before executing `/init.d`. Both check `/local` at `grpc://localhost:${GRPC_PORT}`,
the same target used by SQL and compressed SQL init files.

## Behaviour

The image configures a 10-second Docker health interval, a 60-second startup
grace period, a 12-second timeout and three failures before marking it unhealthy.

Every invocation first acquires the same nonblocking `flock`. The lock stays
held through the cache decision, all RPCs and any cache update or invalidation.
A concurrent invocation fails without issuing RPCs or changing the cache.
Do not remove `readiness.lock` while the container is running.

- `--readiness`, missing cache or expired cache: run SELECT, scheme listing and
  optional CREATE/DROP under `/local/.sys_health`. Each operation must succeed.
- Fresh cache: run one `discovery whoami` RPC. This checks that gRPC responds;
  database/DDL availability is checked again on the next full readiness.
- Failed liveness: invalidate the cache so recovery requires full readiness.

A full readiness does one attempt under a single deadline covering its RPCs and
cache update. Docker and the entrypoint handle retries. Each probe deadline
kills its process group, including children that ignore TERM, to release the
lock. An interrupted CREATE/DROP is recovered by the next successful readiness.

Successful readiness is cached atomically. The record includes boot ID, PID 1
start time, gRPC port and DDL setting. These prevent reuse after restart or a
change of configuration, including when a custom state directory is persistent.
Cache age uses `/proc/uptime`; corrupt and future timestamps require readiness.

## Settings

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `GRPC_PORT` | `2136` | Local gRPC port, also used by startup and init scripts |
| `YDB_LIVENESS_TIMEOUT` | `2s` | Deadline for the liveness RPC |
| `YDB_READINESS_TIMEOUT` | `8s` | Deadline for all readiness RPCs and cache update |
| `YDB_READINESS_ENABLE_DDL` | `true` | Include CREATE/DROP in readiness |
| `YDB_READINESS_INTERVAL_SECONDS` | `60` | Maximum age of cached readiness |
| `YDB_HEALTH_STATE_DIR` | `/dev/shm/ydb_health` | Writable directory private to this container |

Deadlines must be positive. If increasing them, also increase Docker's
`--health-timeout` beyond the longest probe plus scheduling overhead. Increasing
the health interval or readiness cache lifetime delays detection of failures.

## Read-only root filesystem

The probes use Docker's private `/dev/shm` tmpfs, which is writable with
`--read-only`. If it is unavailable or shared, set `YDB_HEALTH_STATE_DIR` to
another writable directory private to the container. An unwritable directory
fails the check; an unwritable cache is never trusted because a failed liveness
could not invalidate it. The probes do not require a writable `/tmp`.

The YDB launcher needs temporary files, and the server needs writable storage.
This command also gives startup a volume for generating certificates:

```sh
docker run --read-only \
  --tmpfs /tmp:rw,nosuid,nodev \
  --volume ydb-data:/ydb_data \
  --volume ydb-certs:/ydb_certs \
  your-local-ydb-image
```

## Tests

```sh
python3 .github/docker/tests/test_healthcheck.py
IMAGE=your-local-ydb-image EXPECTED_REVISION=your-ydbd-git-sha \
  bash .github/docker/tests/run_acceptance_tests.sh
```

The probe suite runs with writable and read-only root filesystems in disposable
Ubuntu containers. Only `/ydb` is replaced to inject failures; `timeout`, `flock`,
procfs and container restarts are real. Barriers cover both orders of overlapping
cached and full probes. The `Docker healthcheck` workflow runs this suite.
Image acceptance covers real YDB, init scripts and read-only-rootfs restarts.
