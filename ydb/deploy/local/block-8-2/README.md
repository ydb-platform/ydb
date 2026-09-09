# Local Block82 acceptance cluster

This single-host functional fixture runs 13 storage nodes and one dedicated compute
node for each of `/Root/block42` and `/Root/block82`. Static group 0 uses Block82 on
nodes 1–12; node 13 is an additional reassign target. Each database owns exactly two
dynamic groups in its own `ssd-block42` or `ssd-block82` pool. The logical racks do
not provide physical failure isolation or a distributed performance benchmark.

Every storage node owns a persistent volume with a 16 GiB sparse PDisk file.
No SectorMap is used. Initializer markers have separate persistent volumes: after
the first successful bootstrap, initializers exit without repeating initialization.
Compute health checks and acceptance read the existing rows after a full restart,
so recreating the smoke tables cannot hide lost data.
Storage bootstrap commits the initial configuration through distconf; automatic
box management then registers all 13 PDisks. Static reassign uses this persisted
configuration, including its generation and donors.

Requirements: Linux x86_64, Docker, Docker Compose with conditional dependencies,
Python 3 on the host, and sufficient RAM/disk for the YDB build and 15 processes.
All published ports bind to host loopback. The image uses the same committed source
for storage, compute and CLI; do not run older binaries on these volumes.

From this directory:

```bash
./build-image.sh
docker-compose up -d
python3 acceptance.py --output /tmp/block82-acceptance --restart
docker-compose ps
```

`build-image.sh` requires committed source, builds with `relwithdebinfo`, and writes
the immutable local image ID to `.env`. Its OCI revision label identifies the
source commit. `generate.py` regenerates the checked-in configuration and Compose
file without external Python dependencies. A storage initializer registers the
host configurations and box in BSC. Two database initializers then create their
database, table and deterministic row on first startup.

| Database | gRPC endpoint | Monitoring |
| --- | --- | --- |
| `/Root/block42` | `grpc://127.0.0.1:2136` | <http://127.0.0.1:8766/> |
| `/Root/block82` | `grpc://127.0.0.1:2137` | <http://127.0.0.1:8767/> |

- Cluster: <http://127.0.0.1:8765/monitoring/cluster>
- Block42 health: <http://127.0.0.1:8765/monitoring/tenant/healthcheck?name=/Root/block42>
- Block82 health: <http://127.0.0.1:8765/monitoring/tenant/healthcheck?name=/Root/block82>

Read either database using the matching endpoint:

```sql
SELECT id, value FROM smoke ORDER BY id;
```

Expected rows are `(1, "block42-persistent")` and `(1, "block82-persistent")`.
The acceptance script saves image/container provenance, both Viewer responses,
tenant node counts, histogram buckets and SQL results before and after restarting
the entire Compose project. It leaves all 15 servers and volumes running.

Safe stop, preserving every volume:

```bash
docker-compose stop
```

Restart the stopped cluster with `docker-compose up -d`. To intentionally destroy
this fixture and all its data, use `docker-compose down -v`; do not use that command
as part of acceptance or before the user has reviewed the running cluster.
