# Split/merge load

A tool for generating a controlled stream of [partition](../../concepts/datamodel/table.md#partitioning)/shard [splits](../../concepts/glossary.md#split) and [merges](../../concepts/glossary.md#merge) in row-oriented tables (the split-merge mechanics is row-table specific). It is designed to exercise the [auto-partitioning](../../concepts/glossary.md#auto-partitioning) mechanics: the split/merge queue, the in-flight limit on split-merge operations, and the fair scheduler under sustained pressure.

Unlike the other workloads, its goal is not throughput measurement but creating a controlled, reproducible split-merge load on tables: by exceeding the data-size threshold, by exceeding the CPU threshold (split-by-load), a full merge cascade with a single `ALTER TABLE`, or oscillating between these states.

The key insight for merge testing: **no data deletion is needed**. A table already split into many small partitions, followed by a single `ALTER TABLE` raising `AUTO_PARTITIONING_PARTITION_SIZE_MB`, produces a [merge cascade](../../concepts/glossary.md#merge-cascade) — the raised threshold makes neighboring partitions eligible for merging immediately.

The defaults are deliberately tiny (`AUTO_PARTITIONING_PARTITION_SIZE_MB = 1`, payload ~1 KB): a few thousand rows already cross the thresholds, so pressure is reachable without gigabytes of data. The defaults are not production-like; the settings should be adjusted for the tested scenario.

## Types of load {#workload-types}

This workload runs several types of load. Each primitive is named after the signal that triggers the server-side operation: **by-size** — accumulated data size crossing `AUTO_PARTITIONING_PARTITION_SIZE_MB`; **by-load** — sustained CPU load on a shard (enabled by `AUTO_PARTITIONING_BY_LOAD`). The workload generates the traffic; the server decides when to split/merge:

* [split-by-size](#split-by-size): Grows partitions past the `AUTO_PARTITIONING_PARTITION_SIZE_MB` threshold via sequential fresh-row writes.
* [split-by-load](#split-by-load): Hot-spot point UPSERT traffic on a narrow key slice; CPU grows while data size stays flat, triggering [split by load](../../concepts/glossary.md#split-by-load) — splits driven by sustained CPU load on a shard, enabled by the `AUTO_PARTITIONING_BY_LOAD` setting.
* [merge-by-size](#merge-by-size): Raises the partition size threshold with a single `ALTER TABLE` (issued once per run), then waits out the merge cascade.
* [merge-by-load](#merge-by-load): Run after the hot traffic stops; after split-by-load, the load on the hot shard decays and merges are triggered by the load drop; the mode polls and prints the decreasing partition count.
* [split-burst](#split-burst): Pushes all shards past the size threshold simultaneously (split-by-size on all shards at once); requires `--key-distribution striped` (without it, the mode degrades to sequential split-by-size) so that each query hits a different region of the key space.
* [merge-burst](#merge-burst): A full merge-by-size cascade as fast as the server's split-merge limits allow (a single ALTER raises the size threshold, then the cascade drains at limit speed).
* [multi-table-split](#multi-table-split): Many tables split in parallel, competing for the shared resources of the split-merge machinery.
* [small-table-starvation](#small-table-starvation): A small demanding table (split-by-load traffic on the small table) competing with a big dormant table for the shared resources of the split-merge machinery.
* [split-vs-merge-race](#split-vs-merge-race): A merge-by-size wave racing concurrent split-by-size demand (run together with `merge-by-size` in a second process).
* [flap](#flap): Alternates grow/merge-by-size/split-by-size phases on one table (the size threshold is raised and lowered via ALTERs).
* [status](#status): Polls the first table (`<path>0`) once per second and prints the dynamics of the partition count, key interval widths, and row distribution.

## Load test initialization {#init}

To get started, the test tables need to be created:

```bash
{{ ydb-cli }} workload splitmerge init [init options...]
```

* `init options`: [Initialization options](#init-options).

View a description of the command to initialize the tables:

```bash
{{ ydb-cli }} workload splitmerge init --help
```

### Available parameters {#init-options}

Parameter name | Parameter description
---|---
`--path <value>` | Table name prefix. Tables are created as `<path>0` ... `<path>N-1`. Default: `splitmerge`.
`--tables <value>` | Number of tables to create. Default: 1.
`--initial-partitions <value>` | `UNIFORM_PARTITIONS` per table: a single N for all tables, or a comma-separated list (for example, `"4,64"`). Default: 4.
`--min-partitions <value>` | `AUTO_PARTITIONING_MIN_PARTITIONS_COUNT`: a single N for all tables, or a comma-separated list. Default: 1.
`--max-partitions <value>` | `AUTO_PARTITIONING_MAX_PARTITIONS_COUNT`: a single N for all tables, or a comma-separated list. Default: 256.
`--partition-size <value>` | `AUTO_PARTITIONING_PARTITION_SIZE_MB`, the split-phase size threshold. Default: 1.
`--auto-partition <value>` | Enables/disables `AUTO_PARTITIONING_BY_LOAD`: 0 or 1, a single N for all tables, or a comma-separated list (for example, `"1,0"`). Default: 0.
`--cpu-threshold <value>` | Accepted but has no effect on this server version; the server-side default split-by-load CPU threshold applies. Kept for forward compatibility. Default: 1.

The following command is used to create a table:

```yql
CREATE TABLE `<path>0`(
    k Uint64,
    payload String,
    PRIMARY KEY(k)) WITH (
        STORE = ROW,
        AUTO_PARTITIONING_BY_LOAD = ENABLED, -- if --auto-partition 1
        UNIFORM_PARTITIONS = partsNum,
        AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = minPartsNum,
        AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = maxPartsNum,
        AUTO_PARTITIONING_PARTITION_SIZE_MB = sizeMb);
```

Example: create two tables — a small one (4 partitions, split-by-load enabled) and a big dormant one (64 partitions, pinned with `AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 64`):

```bash
{{ ydb-cli }} workload splitmerge init --tables 2 --initial-partitions "4,64" --min-partitions "1,64" --auto-partition "1,0" --partition-size 1
```

## Running the load {#run}

One of the load types is run:

```bash
{{ ydb-cli }} workload splitmerge run [workload type...] [global workload options...] [specific workload options...]
```

* `workload type`: One of the [load types](#workload-types).
* `global workload options`: [Global workload parameters](commands/workload/index.md#global_workload_options).
* `specific workload options`: [Run options](#run-options).

View the description of the load run command:

```bash
{{ ydb-cli }} workload splitmerge run --help
```

### Available parameters {#run-options}

Parameter name | Parameter description
---|---
`--path <value>` | Table name prefix, must match init. Default: `splitmerge`.
`--tables <value>` | Number of tables, must match init. Default: 1.
`--table <value>` | Run against an existing table instead of the init-created `<path>N` tables. Repeatable or comma-separated. See [Running against an existing table](#existing-table).
`--key-column <value>` | Name of the single `Uint64` primary-key column (existing-table mode). Default: `k`.
`--payload-column <value>` | Name of the payload column (existing-table mode). Default: `payload`.
`--payload-type <value>` | Type of the payload column (existing-table mode): `string`, `utf8`, `uint64`, or `int64`. Default: `string`.
`--start-key <value>` | First key for sequential writes (existing-table mode); avoids overwriting existing rows. Default: 0.
`--initial-partitions <value>` | `UNIFORM_PARTITIONS` from init; positions the targeted hot key range. A single N or a comma-separated list.
`--min-partitions <value>` | `AUTO_PARTITIONING_MIN_PARTITIONS_COUNT` (the merge-phase floor): a single N or a comma-separated list.
`--partition-size <value>` | Split-phase partition size, MB (used by the flap split phase). Default: 1.
`--partition-size-high <value>` | Merge-phase partition size, MB (used by merge modes and the flap merge phase). Default: 64.
`--rows <value>` | Rows per write query. Default: 1000.
`--len <value>` | Payload string length, bytes. Default: 1024.
`--key-distribution <value>` | Write key distribution: `sequential` (default), `targeted`, or `striped`.
`--target-shards <value>` | Shards to concentrate traffic on (targeted mode): comma-separated indices or `random`. Default: 0.
`--target-share <value>` | Share of each target shard's key range receiving traffic, percent (1..100). Default: 1.
`--concurrent-phases` | Flap: run phases concurrently (the merge ALTER is issued while writes continue).
`--phase-time <value>` | Max duration of one flap phase, seconds. Default: 60.
`--cycles <value>` | Number of flap cycles. Default: 3.
`--max-rows <value>` | Hard stop on total rows written. Default: unlimited.

### How the traffic-shape knobs map to pressure types

The traffic-shape options are orthogonal to the pressure signal: the run type chooses *which* stat gets pushed past a threshold, the distribution options choose *which* shards feel it.

| Mode / signal | `--key-distribution` | `--target-shards` / `--target-share` | Effect |
|---|---|---|---|
split-by-size (default) | `sequential` | — | Monotonic keys fill shards one after another; each crosses the size threshold in turn — rolling split demand. |
split-by-size, targeted variant | `targeted` | `0`, share 50 | Fresh rows concentrated in half of shard 0's range — it crosses the size threshold quickly — single-shard split demand. |
split-by-load | `targeted` | one shard, share 1 | Point UPSERTs over a fixed hot key set (1% of one shard's range) — its CPU crosses the load threshold — split-by-load, size untouched. |
split-burst | `striped` | — | All shards written simultaneously — all cross the size threshold together — mass simultaneous demand. |
merge-by-size | (irrelevant) | — | Traffic shape doesn't matter; the ALTER raises the size threshold and the merge cascade follows. |
merge-by-load | `targeted`, then stop | — | The load on the hot shard falls below the load threshold and merges are triggered; the cascade progress is tracked by polling the partition count. |

Two consequences:

* `sequential` and `striped` **cannot** produce split-by-load pressure: sequential upserts move the write point across shards as ranges fill (no sustained hot spot), striped spreads CPU thin across all shards. split-by-load **requires** `targeted` with a small `--target-share` so one shard's CPU concentrates.
* By-size works with **any** distribution — the signal only needs accumulated bytes. `targeted` makes one shard cross the threshold faster; `striped` makes all shards cross together; `sequential` produces rolling demand.

split-by-load modes write **in-place UPSERTs over a fixed hot key set**, never fresh rows: fresh keys grow the shard's data size and can trigger unwanted size-based splits alongside the load-based ones. The first iteration creates the rows; subsequent iterations rewrite them, so CPU grows while data size stays flat.

### Per-table policy lists

`--initial-partitions`, `--min-partitions`, `--max-partitions`, and `--auto-partition` each accept either a single value (applied to all tables) or a comma-separated list (one value per table). Per-table lists are the general mechanism for asymmetric scenarios — for example, a small table with split-by-load enabled next to a big table pinned at a high `AUTO_PARTITIONING_MIN_PARTITIONS_COUNT` (see [small-table-starvation](#small-table-starvation)).

### Running against an existing table {#existing-table}

The `run` command can target an existing table instead of the init-created ones by passing `--table` (repeatable, or a comma-separated list). This makes it possible to exercise split/merge pressure on a table with real data, a production-like schema, or a pre-set partitioning policy — for example, a large table pinned at a high `AUTO_PARTITIONING_MIN_PARTITIONS_COUNT`.

Compatibility requirements:

* The table must have a single `Uint64` primary-key column (its name is passed via `--key-column`).
* The table must have a writable payload column (its name and type are passed via `--payload-column` and `--payload-type`).
* By-load modes require `AUTO_PARTITIONING_BY_LOAD` enabled on the table.

At startup the tool validates the schema via `DescribeTable` and stops the run with a clear error if the table is not compatible. The actual partition count observed at validation time is used for the hot-key-range math, so `--initial-partitions` is not needed for existing tables.

Example — split-by-load against a table with `id Uint64` as the key and `value Uint64` as the payload:

```bash
{{ ydb-cli }} workload splitmerge run split-by-load --seconds 60 \
  --table my_table --key-column id --payload-column value --payload-type uint64
```

{% note warning %}

In existing-table mode the tool **writes data** into the table and **issues ALTERs** on it (merge modes change `AUTO_PARTITIONING_PARTITION_SIZE_MB` and `AUTO_PARTITIONING_MIN_PARTITIONS_COUNT`). Sequential writes start at `--start-key` (default 0); use it to avoid overwriting existing rows. The `init` and `clean` commands do not apply to `--table` targets and never drop them.

{% endnote %}

### Reproducing modes manually

Every higher-level mode is a composition of the four primitives (`split-by-size`, `split-by-load`, `merge-by-size`, `merge-by-load`) plus the traffic-shape and policy options — the same scenarios can be reproduced by hand with separate CLI invocations and shell orchestration. The built-in modes exist for convenience, reproducible timing (concurrent phases are hard to script with separate processes), and the built-in status reporting.

### split-by-size {#split-by-size}

Sequential fresh-row writes grow the table past `AUTO_PARTITIONING_PARTITION_SIZE_MB`, triggering size splits:

```bash
{{ ydb-cli }} workload splitmerge run split-by-size --seconds 60
```

### split-by-load {#split-by-load}

Requires a table initialized with `--auto-partition 1`. Point UPSERT traffic over a fixed hot key set: CPU grows while data size stays flat, triggering split-by-load. The hot key set is a configurable share of one shard's key range:

```bash
{{ ydb-cli }} workload splitmerge run split-by-load --initial-partitions 4 --target-shards 0 --target-share 1
```

`--target-shards random` picks a random shard; a comma-separated list (for example, `--target-shards 0,2`) rotates traffic across several shards.

### merge-by-size {#merge-by-size}

Raises `AUTO_PARTITIONING_PARTITION_SIZE_MB` to `--partition-size-high` with a single `ALTER TABLE` (issued once per run), then waits out the merge cascade without re-issuing the ALTER:

```bash
{{ ydb-cli }} workload splitmerge run merge-by-size --min-partitions 1 --partition-size-high 64 --seconds 120
```

### merge-by-load {#merge-by-load}

After a [split-by-load](#split-by-load) run and once the traffic stops, the load on the hot shard decays below the load threshold and merges are triggered by the load drop. Unlike merge-by-size, which raises the size threshold with an `ALTER TABLE`, merge-by-load changes nothing: the size threshold stays put, and the merges come from the load signal alone. While the server performs the merges, the mode polls the table's partition count once per second and prints a line of the form `merge-by-load\tpartitions\t<path>\t<count>`, and after several consecutive unchanged polls — a final summary line `merge-by-load\tdone\t<path>\t<final count>`, so the cascade drain can be verified from the output:

```bash
{{ ydb-cli }} workload splitmerge run merge-by-load --seconds 120
```

### split-burst {#split-burst}

With `--key-distribution striped`, each query takes a block from a different region of the key space, pushing all shards past the size threshold simultaneously. Without `--key-distribution striped`, the mode degrades to sequential split-by-size:

```bash
{{ ydb-cli }} workload splitmerge run split-burst --key-distribution striped --seconds 60
```

### merge-burst {#merge-burst}

A full merge cascade as fast as the server's split-merge limits allow: the same single-ALTER-then-wait behavior as [merge-by-size](#merge-by-size), intended to be run right after a large split burst:

```bash
{{ ydb-cli }} workload splitmerge run merge-burst --min-partitions 1 --partition-size-high 64 --seconds 120
```

### multi-table-split {#multi-table-split}

Round-robin fresh-row writes across all tables so they compete for the shared resources of the split-merge machinery in parallel. Requires `--tables N > 1` at init:

```bash
{{ ydb-cli }} workload splitmerge run multi-table-split --tables 3 --seconds 60
```

### small-table-starvation {#small-table-starvation}

A small demanding table (split-by-load enabled) competing with a big dormant table. Initialization is done with per-table lists so that the big table stays big:

```bash
{{ ydb-cli }} workload splitmerge init --tables 2 --initial-partitions "4,64" --min-partitions "1,64" --auto-partition "1,0" --partition-size 1
{{ ydb-cli }} workload splitmerge run small-table-starvation --tables 2 --initial-partitions "4,64" --seconds 60
```

### split-vs-merge-race {#split-vs-merge-race}

A merge wave racing concurrent split demand. The split demand side is run in one process, and the merge side ([merge-by-size](#merge-by-size)) concurrently in a second process:

```bash
{{ ydb-cli }} workload splitmerge run split-vs-merge-race --seconds 60 &
{{ ydb-cli }} workload splitmerge run merge-by-size --min-partitions 1 --partition-size-high 64 --seconds 60
```

### flap {#flap}

Alternates grow/merge/split phases on one table: each phase lasts at most `--phase-time` seconds, one `ALTER TABLE` per phase, `--cycles` cycles in total. The merge phase raises the size threshold (`--partition-size-high`), the split phase lowers it back (`--partition-size`). With `--concurrent-phases`, the merge ALTER is issued once during the first grow phase while writes continue:

```bash
{{ ydb-cli }} workload splitmerge run flap --phase-time 60 --cycles 3 --seconds 600
```

### status {#status}

Polls the first table (`<path>0`) once per second and prints per-poll dynamics of the partition count, key interval width distribution, and per-partition row distribution:

```text
partitions  /Root/db/splitmerge0  13  widths  min=1  med=1  max=uint64_max  rows  min=0  med=46  max=97
```

The last partition has no upper key bound, so its width is unbounded and printed as `uint64_max`. Fields in the output line are tab-separated.

Narrow intervals clustered around the hot range are the expected signature of split-by-load; wide uniform intervals, of merges. The mode is run in a second process alongside any load mode:

```bash
{{ ydb-cli }} workload splitmerge run status --seconds 120
```

## Practical notes {#practical-notes}

* **Merge-by-size modes (merge-by-size, merge-burst) issue the ALTER exactly once per run**, then wait out the cascade. The run duration should exceed the expected cascade drain time (`--seconds` greater than that time); restarting the mode re-issues the ALTER, which only adds server load without changing the outcome.
* **`--max-rows` is the clean stop for write modes**: the run stops at the first query boundary at or after the limit, so the total may exceed it by less than one query's worth of rows (`--rows`); this can be used for reproducible data volumes. `--seconds` stops the run at a wall-clock deadline regardless of write progress.
* **Transient errors during active splitting or merging are expected**: while partitions are actively moving, multi-threaded write runs may show a small number of `Retries`/`Errors` in the run's progress statistics (shard overload, stale partition boundaries; the driver retries automatically). This is server-side behavior under partition movement, not a tool failure; single-threaded runs (`--threads 1`) are usually clean.

## Cleaning up {#clean}

Deleting the test tables is done with the command:

```bash
{{ ydb-cli }} workload splitmerge clean [clean options...]
```

* `clean options`: [Cleanup options](#clean-options).

### Available parameters {#clean-options}

Parameter name | Parameter description
---|---
`--path <value>` | Table name prefix, must match init. Default: `splitmerge`.
`--tables <value>` | Number of tables to remove; the value should be greater than or equal to the number created to remove them all (missing tables are skipped). Default: 1.

Example:

```bash
{{ ydb-cli }} workload splitmerge clean --tables 2
```
