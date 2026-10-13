# YQ-4764: missing S3 objects during a read

## Proposed SQL contract

Use one read option, `ON_MISSING_FILE`, in the existing `WITH` clauses. The
accepted string values are `"fail"`, `"ignore"`, and `"warn"` (case insensitive).
Omission means `"fail"`, preserving current behavior. Reject any other value
at query compilation or external-table creation with a `BAD_REQUEST` issue.
The setting applies only to `ObjectStorage` reads; it does not change writes.

```sql
CREATE EXTERNAL TABLE daily_events (
    event_id Uint64 NOT NULL,
    payload Utf8
) WITH (
    DATA_SOURCE = "events_bucket",
    LOCATION = "daily/",
    FORMAT = "parquet",
    ON_MISSING_FILE = "warn"
);

SELECT *
FROM events_bucket.`daily/`
WITH (
    FORMAT = "parquet",
    SCHEMA = (event_id Uint64 NOT NULL, payload Utf8),
    ON_MISSING_FILE = "ignore"
);
```

The external-table property is stored with that table and should round-trip
through `SHOW CREATE EXTERNAL TABLE`. An ad hoc source read uses the value in
its own `WITH` clause. `CREATE EXTERNAL DATA SOURCE` has no file policy: the
policy belongs to a read, not a connection. No pragma is added.

| Value | On a confirmed missing object | Diagnostics |
| --- | --- | --- |
| `fail` (default) | Fail the query after normal retries. | Existing error. |
| `ignore` | Finish this object as skipped and continue other objects. | Log and counter. |
| `warn` | Same result behavior as `ignore`. | Path-aware warning issues for the first 16 skipped objects per reader, plus log and counter for every skip. |

This is a partial-result option. Complete rows or Parquet row groups already
emitted from a file before it disappears cannot be retracted; an incomplete
record or row group must never be emitted. Document this explicitly for
aggregations and `INSERT ... SELECT`. Missing files can therefore affect
results even when the query reports success.

### Error boundary

Treat an object as missing only after normal retries are exhausted and either
the read returns HTTP 404 with S3 `NoSuchKey` (or an unambiguous 404 for that
object), or a truncated read is followed by a lightweight authenticated GET
of the same key that returns 404. The extra probe is needed because an early
EOF or `CURLE_PARTIAL_FILE` alone can also be a network/server failure. A
short successful range response must be checked against its requested byte
count. A 403, missing bucket, 5xx, timeout, malformed data, or a successful
probe remains an error for every mode. Do not suppress an Arrow/CSV parser
error merely because it mentions EOF. A missing file found while enumerating
or inferring a schema should be handled consistently; other listing and
inference failures remain errors.

## Comparable systems

* Apache Spark has `ignoreMissingFiles` as a file-source option and a session
  setting, both defaulting to false. Its documentation covers a file deleted
  after DataFrame construction. `FileScanRDD` catches `FileNotFoundException`,
  logs the skipped path, and continues; it keeps data already read. Spark has
  a separate `ignoreCorruptFiles` option, which supports keeping YDB's
  missing-object classification narrow. [Spark file options](https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html),
  [Spark reader implementation](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FileScanRDD.scala).
* Trino's Hive connector has `hive.ignore-absent-partitions`, default false,
  for a partition whose location no longer exists. It skips potentially
  expected data. This is a coarser boundary than the proposed YDB per-object
  behavior. [Trino Hive connector](https://trino.io/docs/current/connector/hive.html).

## Implementation route in this checkout

1. **DDL and validation.** Add a dedicated `on_missing_file` field (field 6)
   to `NKikimrExternalSources::TObjectStorage` in
   `ydb/core/protos/external_sources.proto`. In
   `ydb/core/external_sources/object_storage.cpp`, accept the new attribute in
   `Pack`, validate the three values, and expose it from `GetParameters` so
   external-table reads and `SHOW CREATE` receive it. Keep the absent field as
   `fail` for stored tables created before the change. Add positive/negative
   tests in `object_storage_ut.cpp`, SQL validation tests in
   `ydb/core/kqp/ut/federated_query/s3/kqp_federated_query_ut.cpp`, and
   `SHOW CREATE` round-trip tests in `ydb/core/sys_view/ut_show_create.cpp`.
2. **Ad hoc reads and task propagation.** Accept normalized `onmissingfile`
   in `ydb/library/yql/providers/s3/provider/yql_s3_datasource_type_ann.cpp`,
   validating its value. The existing path in `yql_s3_dq_integration.cpp`
   copies read settings into `NS3::TSource.Settings`; verify the option reaches
   both a direct `FROM ... WITH` read and an external-table read. Decode the
   setting once in `CreateS3ReadActor`, using a small shared enum; avoid
   repeated string decisions in download callbacks. No SQL grammar change
   should be needed because the `WITH` settings are generic.
3. **Classify at the transport boundary.** Preserve structured HTTP and S3
   status in range and streaming result events. Apply the option after retry
   exhaustion in `yql_s3_read_actor.cpp` and `yql_s3_raw_read_actor.cpp`.
   For streaming, compare received bytes with the known object size when the
   read is not intentionally stopped by `LIMIT`; probe the key on a short or
   partial transfer. For Parquet range reads, reject a short 206 response and
   classify a 404 range response before it becomes a generic Arrow exception.
   Ensure cancellation and decompressor cleanup complete normally when a
   file is skipped.
4. **Completion and warnings.** Report a skipped file to the parent source
   actor as a completed split so `ListedFiles`, `CompletedFiles`, in-flight
   byte accounting, and file-queue completion stay balanced. Count skipped
   files and emit path-aware logs. In `warn`, send at most 16 `S_WARNING`
   diagnostics per reader through the existing nonfatal async-input issue
   path before source completion. Preserve those warnings across compute actor
   state reports and copy them into the successful KQP response.
5. **Tests and docs.** Cover default, ignore, and warn for external tables and
   direct SQL reads. Cover structured and raw reads, a 403 that must still
   fail, and an object that disappears after a partial response. Test DDL
   validation and `SHOW CREATE` round-tripping. Document the option and
   partial-result semantics in both English and Russian S3 pages.

Schema inference runs while preparing a query, before its file-reader actors
start. A missing file selected for inference remains a schema-inference error;
the policy applies to query reads of files whose schema is already known.

## Main risk to resolve during implementation

The current streaming reader throws `TS3ReadError` before parser EOF checking,
but the whole-object and Parquet paths report errors differently. The option
must be implemented in all three paths or a table's behavior will depend on
format. The existing nonfatal issue buffer is bounded, but is drained on
intermediate compute-actor state reports. The async compute actor now retains
warnings until its final success state, and the KQP executor adds them to the
client response. The end-to-end tests cover this path but still need to run
in an environment with the KQP test binary built.
