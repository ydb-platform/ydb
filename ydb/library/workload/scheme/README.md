# Scheme workload

`ydb workload scheme` stresses schema operations through the public table and topic APIs.
Optional `init` creates an initial set of empty row tables and topics.
`run` creates missing objects while alternately adding and dropping columns or consumers on ready objects.
Both commands take the same YAML file:

```yaml
path: scheme_load_001
create_concurrency: 16
alter_concurrency: 16
tables:
  initial_count: 20
  count: 100
  partitions: 16
  alter_batch_size: 8
topics:
  initial_count: 10
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
`initial_count` is the number prepared by `init`, from 0 to `count`, and defaults to `count`.
If all initial counts are zero, `init` only prepares the directory.
`count` is the total target, including existing objects. `run` fills gaps in the actual set, independently of `initial_count`.
The example prepares 20 tables and 10 topics, then creates the remaining 80 tables and 40 topics during `run`.
Omit `init` to start from an empty directory; prepare the full target to run ALTER traffic only.

CREATE and ALTER concurrency limits are independent and shared across both object types.
For example, limits of 16 and 32 allow up to 48 mutations in flight, subject to available objects.
An object becomes available for ALTER immediately after its CREATE succeeds, without waiting for other CREATEs.
Concurrency does not prescribe a fixed operation ratio or strict CREATE/ALTER alternation.
Each ALTER changes the whole batch in one request, and mutations of an object are sequential.
Tables have a non-nullable `id Uint64` primary key; ALTER adds/drops nullable `extra_<i> Utf8` columns.
Topics initially have no consumers; ALTER adds/drops `reader_<i>` consumers.
Automatic partitioning is disabled. Tables use uniform initial partitioning; topics use a fixed active partition count.

Objects are named `<path>/table_<i>` and `<path>/topic_<i>`. `init` rejects existing objects it is asked to create.
Both commands prepare the directory if needed. Before `run`, its contents are listed and existing object schemas are inspected.
A complete existing ALTER batch is dropped first;
an absent batch is added first. Partial batches or incompatible schemas fail validation before mutations start.
The duration covers CREATE and ALTER after validation; outstanding requests are allowed to finish after it expires.
Reaching `count` stops CREATE for that type; ALTER continues until the duration expires.
The deadline can leave some objects uncreated. A later `run` inspects the actual set and creates the remaining objects.
Each request has a two-minute client timeout. Failed requests stop new work; uncertain DDL outcomes are not retried.
Already completed changes remain, and timed-out requests can still complete on the server.
Use a separate path for each independent experiment; do not run multiple writers against the same objects.

The command reports successful request counts by operation and total elapsed time, including setup and draining.
An error or interrupt exits with a nonzero status and leaves the objects available for inspection.
Memory, SchemeShard counters and OOM evidence are collected separately from the server.
