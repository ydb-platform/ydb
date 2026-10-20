# Table data compression

{{ ydb-short-name }} lets you compress table data on disk to reduce storage usage. Compression settings differ for [row-oriented](datamodel/table.md#row-oriented-tables) and [column-oriented](datamodel/table.md#column-oriented-tables) tables. YQL syntax is documented in [column families](../yql/reference/syntax/create_table/family.md) and the `COMPRESSION` option in [CREATE TABLE](../yql/reference/syntax/create_table/index.md).

## Row-oriented tables {#row-oriented-tables}

In row-oriented tables, compression is configured at the [column group](datamodel/table.md#column-groups) level. To compress rarely read fields or store them on a different storage tier, you can place columns in separate groups.

Each column group has a `COMPRESSION` codec. Allowed values:

* `off` — no compression;
* `lz4` — [LZ4](https://en.wikipedia.org/wiki/LZ4_(compression_algorithm)).

Column groups in row-oriented tables do **not** support other algorithms, including `zstd`. Primary key columns always belong to the `default` group and cannot be moved to another group.

Changes to compression codecs and storage types are not applied immediately to data already on disk: new settings take effect during subsequent background [LSM compaction](glossary.md#compaction). After `ALTER TABLE`, disk space savings appear gradually.

Creating and changing groups: [CREATE TABLE — column families](../yql/reference/syntax/create_table/family.md), [ALTER TABLE — column families](../yql/reference/syntax/alter_table/family.md).

## Column-oriented tables {#column-oriented-tables}

In column-oriented tables, compression is set **per column** with `COMPRESSION(algorithm=…[, level=…])` when [creating](../yql/reference/syntax/create_table/index.md) or [altering](../yql/reference/syntax/alter_table/columns.md) a table.

Parameters:

* `algorithm` — `off`, `lz4`, or `zstd`;
* `level` — compression level for the `zstd` algorithm; allowed values are `0` to `22`. This parameter is not used for other algorithms.

If the `COMPRESSION()` option is specified without parameters, the column uses the default compression. Currently, the default is `lz4`.

Columnar storage often achieves strong compression because values in a column tend to be similar. You can also configure [column encoding](../yql/reference/syntax/create_table/index.md) separately from compression with the `ENCODING` option.

## Algorithm comparison {#algorithm-comparison}

| Algorithm | Available for | Properties |
| --------- | ------------- | ---------- |
| `lz4` | Row-oriented and column-oriented tables; in row-oriented tables, via column groups | Fast algorithm with low CPU overhead |
| `zstd` | Column-oriented tables only | Supports compression `level`; typically achieves higher compression than `lz4` |
| `off` | Row-oriented and column-oriented tables | Compression disabled in {{ ydb-short-name }} |

## Examples {#examples}

Row-oriented table: frequently read columns stay uncompressed; large JSON uses `lz4` in a separate group. Allowed `DATA` values depend on cluster configuration — see [CREATE TABLE — column families](../yql/reference/syntax/create_table/family.md).

```yql
CREATE TABLE user_profiles (
    user_id Uint64,
    display_name Utf8,
    profile_json Utf8 FAMILY payload,
    PRIMARY KEY (user_id),
    FAMILY default (
        DATA = "ssd",
        COMPRESSION = "off"
    ),
    FAMILY payload (
        DATA = "ssd",
        COMPRESSION = "lz4"
    )
);
```

Column-oriented table with different compression algorithms for different columns:

```yql
CREATE TABLE analytics_events (
    event_id Uint64 NOT NULL,
    event_type Utf8 COMPRESSION(algorithm=lz4),
    event_data Json COMPRESSION(algorithm=zstd, level=3),
    event_time Timestamp,
    PRIMARY KEY (event_id)
)
WITH (STORE = COLUMN);
```

Enable compression for the `default` group, for example if you later need to compress frequently read columns too:

```yql
ALTER TABLE user_profiles ALTER FAMILY default SET COMPRESSION "lz4";
```

Move `profile_json` to a group on `rot` storage:

```yql
ALTER TABLE user_profiles
    ADD FAMILY cold (DATA = "rot", COMPRESSION = "lz4"),
    ALTER COLUMN profile_json SET FAMILY cold;
```

## Checking settings {#checking-settings}

Compression and column group settings are part of the table DDL. They are returned by the YQL command [{#T}](../yql/reference/syntax/show_create.md):

```yql
SHOW CREATE TABLE user_profiles;
```

In [{{ ydb-short-name }} CLI](../reference/ydb-cli/index.md), for a row-oriented table use [`scheme describe`](../reference/ydb-cli/commands/scheme-describe.md): the output includes a `Column families` section with a `Compression` field. You can also view the table schema in [{{ ydb-ui-name }}](../reference/ydb-ui/index.md).

## See also {#see-also}

* [Tables](datamodel/table.md) — column groups, columnar storage.
* [{#T}](datamodel/topic.md#message-codec) — message compression in topics.
* [{#T}](query_execution/federated_query/s3/formats.md#compression) — file compression in S3 for federated queries.
