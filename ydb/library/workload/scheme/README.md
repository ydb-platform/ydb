# Scheme workload

`ydb workload scheme` stresses schema operations through the public table, topic and scheme APIs.
Optional `init` creates an initial set of empty row tables and topics.
`run` executes the selected operations until `--seconds` expires or no work remains.

```yaml
path: scheme_load_001
operations:
  create: 16
  alter: 16
  describe: 32
  list: 1
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
ydb -e grpc://localhost:2135 -d /local workload scheme run --config scheme.yaml --seconds 60 --threads 10
# Replace the entire operation set for a read-only run on existing objects:
ydb -e grpc://localhost:2135 -d /local workload scheme run --config scheme.yaml --operations describe=32,list=1
# Run without a YAML file:
ydb -e grpc://localhost:2135 -d /local workload scheme run --path scheme_load_002 \
  --tables 100 --topics 50 --partitions 16 --operations create=16,alter=16,describe=32
```

## Operations

Each `operations` value is a positive limit on unfinished operations, shared across both object types.
Omitted operations are disabled. An empty map, unknown operations and a total concurrency above 256 are rejected.
Limits do not prescribe operation ratios, rates or a strict sequence; the scheduler visits available operation kinds in round-robin order.

| Operation | Behavior |
|---|---|
| `create` | Creates missing objects up to each group's `count`. |
| `alter` | Alternately adds and drops a batch of columns or consumers. |
| `describe` | Repeatedly calls DescribeTable (including partition boundaries) or DescribeTopic. |
| `list` | Repeatedly calls ListDirectory on `path`, independently of object operations. |

Both commands accept `-t` / `--threads` (1 to 256, default 10) for the worker pool.
Workers start SDK calls asynchronously. Concurrency limits can exceed the thread count.
An object becomes available for ALTER and DESCRIBE as soon as its CREATE succeeds.
At most one object operation runs on a given object at a time, including DESCRIBE.
CREATE ends once the target is reached; other enabled operations continue until the deadline.
A CREATE-only run finishes when all objects exist, even before the deadline.

Without `operations`, CREATE and ALTER are enabled with concurrency 1 each.
Existing YAML fields `create_concurrency` and `alter_concurrency` remain supported in this mode.
Combining either legacy field with `operations` in the same YAML file is an error.
`init` always prepares `initial_count` objects regardless of the selected run operations;
it uses CREATE concurrency when configured and otherwise 1. Thus a DESCRIBE-only config can be used for both commands.

## CLI overrides

`--config` is optional. Explicit CLI options override YAML; omitted options preserve YAML values.

| Option | Effect |
|---|---|
| `--path PATH` | Replaces the directory relative to the database. |
| `--tables N`, `--topics N` | Replaces that group's `count`, preserving other fields; adds the group if absent. |
| `--partitions N` | Replaces initial partition counts in all configured groups. |
| `--operations create=16,describe=32` | Replaces the entire operation set, including legacy concurrency fields. |

Groups added through CLI default to one partition and ALTER batch size 1.
Without a file, `--path` and at least one object group are required, except for LIST-only runs.
LIST-only runs need only an existing `path`; configured object groups are not inspected in that mode.
If an explicit `initial_count` exceeds the overridden `count`, validation fails.
Different partition counts per type, `initial_count` and ALTER batch sizes are configured through YAML.
`--seconds` / `-s` (default 60) applies only to `run`. Thread and duration settings are CLI-only.

## Objects and lifecycle

`path` is relative to the database. Either object group can be omitted.
`count` and `partitions` are required positive integers in YAML. Batch sizes default to 1.
`initial_count` ranges from 0 to `count` and defaults to `count`; zero initial counts prepare only the directory.
`count` includes existing objects; `run` discovers the actual set independently of `initial_count`.
Objects are named `<path>/table_<i>` and `<path>/topic_<i>`.
`init` rejects existing objects it is asked to create.

Tables have a non-nullable `id Uint64` primary key; ALTER adds/drops nullable `extra_<i> Utf8` columns.
Topics initially have no consumers; ALTER adds/drops `reader_<i>` consumers.
Each ALTER changes the whole configured batch in one request.
Automatic partitioning is disabled. Tables use uniform initial partitioning; topics use a fixed active partition count.

`init` and runs with CREATE enabled prepare the directory if needed.
Runs without CREATE never create directories or objects; missing required objects fail validation.
Before traffic starts, existing object schemas are inspected and validated, including partition counts and settings.
A complete existing ALTER batch is dropped first; an absent batch is added first.
Partial batches, indexed tables and incompatible schemas fail validation before mutations start.
Preparation and validation requests are excluded from load operation counters and `--seconds`.

At the deadline new work is no longer scheduled; outstanding work is allowed to finish.
The deadline can leave objects uncreated. A later run with CREATE enabled fills the remaining gaps.
Each SDK request has a two-minute client timeout. Errors stop new work; uncertain DDL outcomes are not retried.
Completed changes remain, and timed-out requests can still complete on the server.
Use a separate path for each independent experiment; do not run multiple writers against the same objects.

The command reports successful request counts by operation and elapsed time including setup and draining.
An error or interrupt exits with a nonzero status and leaves objects available for inspection.
Memory, SchemeShard counters and OOM evidence are collected separately from the server.
