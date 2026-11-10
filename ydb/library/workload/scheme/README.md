# Scheme workload

`ydb workload scheme` stresses schema operations through the public table and topic APIs.
`init` creates empty row tables and topics; `run` alternately adds and drops columns or consumers.
Both commands take the same YAML file:

```yaml
path: scheme_load_001
create_concurrency: 16
alter_concurrency: 16
tables:
  count: 100
  partitions: 16
  alter_batch_size: 8
topics:
  count: 50
  partitions: 8
  alter_batch_size: 4
```

```bash
ydb -e grpc://localhost:2135 -d /local workload scheme init --config scheme.yaml
ydb -e grpc://localhost:2135 -d /local workload scheme run --config scheme.yaml --seconds 60
```

`path` is relative to the database. Either object group can be omitted; at least one is required.
`count` and `partitions` are required positive integers. Concurrency and batch sizes default to 1.
Concurrency limits are shared across both object types and capped by the number of objects.
Each ALTER changes the whole batch in one request, and mutations of an object are sequential.
Tables have a non-nullable `id Uint64` primary key; ALTER adds/drops nullable `extra_<i> Utf8` columns.
Topics initially have no consumers; ALTER adds/drops `reader_<i>` consumers.
Automatic partitioning is disabled. Tables use uniform initial partitioning; topics use a fixed active partition count.

Objects are named `<path>/table_<i>` and `<path>/topic_<i>`. `init` rejects existing objects.
Before `run`, all object schemas are inspected. A complete existing ALTER batch is dropped first;
an absent batch is added first. Partial batches or incompatible schemas fail validation before mutations start.
The duration starts after this validation; outstanding requests are allowed to finish after it expires.
Each request has a two-minute client timeout. Failed requests stop new work; uncertain DDL outcomes are not retried.
Already completed changes remain, and timed-out requests can still complete on the server.
Use a separate path for each independent experiment; do not run multiple writers against the same objects.

The command reports successful request counts by operation and total elapsed time, including setup and draining.
An error or interrupt exits with a nonzero status and leaves the objects available for inspection.
Memory, SchemeShard counters and OOM evidence are collected separately from the server.
