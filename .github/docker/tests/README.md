# local-ydb healthcheck tests

Run the probe regression tests with Python 3 and Docker:

```sh
python3 .github/docker/tests/test_healthcheck.py
```

The tests use disposable Ubuntu 22.04 containers, the packaged Bash scripts,
GNU timeout, flock, procfs and real container restarts. Only the `/ydb` executable
is replaced, to inject failures and unresponsive RPCs without starting a database.
They run automatically for Docker changes in the `Docker healthcheck` workflow.
The entire probe suite runs with both writable and read-only root filesystems;
the latter also covers an explicitly configured state path and a read-only cache.

Run the image acceptance tests, including SQL/DDL and init scripts with Docker
healthchecks enabled, after building the image:

```sh
IMAGE=your-local-ydb-image EXPECTED_REVISION=your-ydbd-git-sha \
  bash .github/docker/tests/run_acceptance_tests.sh
```

## Probe contract

Docker invokes `/health_check` every 10 seconds, with a 60-second startup grace
period and a 12-second timeout. Three consecutive failed checks mark it unhealthy.
Before the first readiness success and whenever its cache expires, the checker
runs SELECT, scheme listing and (by default) CREATE/DROP under `.sys_health`.
Between readiness checks, one `discovery whoami` RPC verifies that gRPC responds;
it does not assert that the database or every cluster component is healthy.
Database readiness is checked at least on the first Docker invocation after the
60-second cache expires. A failed liveness probe invalidates that cache.

The entrypoint calls `/health_readiness` directly before running init scripts.
Both callers use the same `flock` lock for DDL; a busy lock returns failure, never
a cached success. Successful readiness is cached atomically. Cache entries include
the boot ID, PID 1 start time, endpoint, database and DDL setting, so they cannot
survive a container restart or a change of target. Cache age uses `/proc/uptime`,
independently of changes to wall-clock time. Do not remove `readiness.lock` while
the container is running: all callers must lock the same inode.

Settings (environment):

| Setting | Default | Meaning |
| --- | --- | --- |
| `GRPC_PORT` | `2136` | Default local gRPC port |
| `YDB_ENDPOINT` | `grpc://localhost:${GRPC_PORT}` | Target endpoint |
| `YDB_DATABASE` | `/local` | Target database |
| `YDB_LIVENESS_TIMEOUT` | `2s` | Deadline for the one liveness RPC |
| `YDB_READINESS_TIMEOUT` | `8s` | Deadline for the entire readiness, including retries and sleeps |
| `YDB_READINESS_RETRIES` | `2` | Maximum attempts within the same total deadline |
| `YDB_READINESS_SLEEP` | `1` | Seconds between readiness attempts |
| `YDB_READINESS_ENABLE_DDL` | `true` | Whether readiness also creates/drops its test table |
| `YDB_READINESS_INTERVAL_SECONDS` | `60` | Maximum age of cached readiness |
| `YDB_HEALTH_STATE_DIR` | `/dev/shm/ydb_health` | Writable, container-private directory for the readiness cache and lock |

Probes kill their process group at the deadline so a stuck child cannot retain
the lock. An interrupted CREATE/DROP may leave the test table, which the next
successful readiness removes. If increasing probe deadlines, also increase
Docker's `--health-timeout` beyond the longest probe plus scheduling overhead.
Zero/unbounded probe deadlines are rejected. Increasing `--health-interval`
also increases failure-detection latency.

## Read-only root filesystem

The probes keep their cache and lock in Docker's private `/dev/shm` tmpfs, which
is writable with `--read-only`. They do not modify the packaged scripts or require
a writable `/tmp`. If `/dev/shm` is unavailable, read-only or shared with another
container, set `YDB_HEALTH_STATE_DIR` to a writable directory private to this
container. All probe invocations, including the entrypoint, must use the same
directory. An unwritable state directory fails readiness; an unwritable cache is
never trusted, because a subsequent liveness failure could not invalidate it.

The YDB server and its launcher also need writable data and temporary files. For
the default configuration, run the image with dedicated volumes and a tmpfs:

```sh
docker run --read-only \
  --tmpfs /tmp:rw,nosuid,nodev \
  --volume ydb-data:/ydb_data \
  --volume ydb-certs:/ydb_certs \
  your-local-ydb-image
```

The certificates volume is writable here because the default startup generates
certificates. A read-only certificate/configuration bind mount is a separate
case: the supplied files must be complete and supported by the image's launcher.
Disabling readiness DDL does not remove the server's need for writable storage.
The image acceptance suite checks startup, init scripts, Docker health and a
restart with a read-only root filesystem.
