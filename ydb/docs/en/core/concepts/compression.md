# Table data compression

{{ ydb-short-name }} compresses table data on disk to reduce storage usage. Compression settings differ for [row-oriented](datamodel/table.md#row-oriented-tables) and [column-oriented](datamodel/table.md#column-oriented-tables) tables. YQL syntax is documented in [column families](../yql/reference/syntax/create_table/family.md) and [column options](../yql/reference/syntax/_includes/column_option_list.md#compression).

## Row-oriented tables {#row-oriented-tables}

In row-oriented tables, compression is configured at the [column group](datamodel/table.md#column-groups) level. To compress rarely read fields or store them on a different storage tier, you can place columns in separate groups.

Each column group has a `COMPRESSION` codec. Allowed values:

* `off` — no compression;
* `lz4` — [LZ4](https://en.wikipedia.org/wiki/LZ4_(compression_algorithm)); fast, with low CPU overhead.

Column groups in row-oriented tables do **not** support other algorithms, including `zstd`. Primary key columns always belong to the `default` group and cannot be moved to another group.

Creating and changing groups: [CREATE TABLE — column families](../yql/reference/syntax/create_table/family.md), [ALTER TABLE — column families](../yql/reference/syntax/alter_table/family.md).

## Column-oriented tables {#column-oriented-tables}

In column-oriented tables, compression is set **per column** with `COMPRESSION(algorithm=…[, level=…])` when [creating](../yql/reference/syntax/create_table/index.md#compression) or [altering](../yql/reference/syntax/alter_table/columns.md#compression) a table.

Parameters:

* `algorithm` — `off`, `lz4`, or `zstd`;
* `level` — compression level for the `zstd` algorithm; allowed values are `0` to `22`. This parameter is not used for other algorithms.

If the `COMPRESSION()` option is specified without parameters, the column uses the default compression. Currently, the default is `lz4`.

Columnar storage often achieves strong compression because values in a column tend to be similar. You can also configure [column encoding](../yql/reference/syntax/_includes/column_option_list.md#encoding) separately from compression with the `ENCODING` option.

## When settings take effect {#when-settings-apply}

Changes to compression codecs and storage types for column groups **do not rewrite existing data immediately**. New settings apply to stored data during background [LSM compaction](glossary.md#compaction).

After an `ALTER TABLE` that changes compression, disk space savings appear gradually as compaction runs. The chosen algorithm affects read and write performance immediately for **new** writes.

## Choosing an algorithm {#choosing-algorithm}

| Algorithm | Available for | When to use |
| --------- | ------------- | ----------- |
| `lz4` | Row-oriented and column-oriented tables; in row-oriented tables, via column groups | Frequent updates when latency and CPU load matter |
| `zstd` | Column-oriented tables only | Large “cold” columns: logs, JSON, rarely read fields. Levels `1`–`5` balance speed and ratio; higher levels suit data that is seldom rewritten |
| `off` | Both models | Application-side compression already, or compression gives no benefit |

For column-oriented tables with heavy writes, you can start with `lz4` or a low `zstd` level. For archival columns, a higher `zstd` level fits after measuring on a representative data volume.

## Examples {#examples}

Row-oriented table: compression via column groups and the `lz4` codec only:

```yql
CREATE TABLE user_profiles (
    user_id Uint64,
    display_name Utf8,
    profile_json Utf8 FAMILY payload,
    PRIMARY KEY (user_id),
    FAMILY default (
        DATA = "ssd",
        COMPRESSION = "lz4"
    ),
    FAMILY payload (
        DATA = "ssd",
        COMPRESSION = "lz4"
    )
);
```

Column-oriented table: different algorithms per column:

```yql
CREATE TABLE analytics_events (
    event_id Uint64 NOT NULL,
    event_type Utf8,
    event_data Json COMPRESSION(algorithm="zstd", level=3),
    event_time Timestamp,
    PRIMARY KEY (event_id)
)
WITH (STORE = COLUMN);
```

Enabling compression on an existing row-oriented table:

```yql
ALTER TABLE logs ALTER FAMILY default SET COMPRESSION "lz4";
```

Adding a column group and moving a column:

```yql
ALTER TABLE logs
    ADD FAMILY archive (DATA = "ssd", COMPRESSION = "lz4"),
    ALTER COLUMN message SET FAMILY archive;
```

## Checking settings {#checking-settings}

The commands below show column group parameters and column options:

```yql
DESCRIBE TABLE your_table;
```

```yql
SHOW CREATE TABLE your_table;
```

## See also {#see-also}

* [Tables](datamodel/table.md) — column groups, columnar storage.
* [Batch upload](../dev/batch-upload.md) — recommended loading pattern for column-oriented tables.
* [{#T}](datamodel/topic.md#message-codec) — message compression in topics.
* [{#T}](query_execution/federated_query/s3/formats.md#compression) — file compression in S3 for federated queries.
