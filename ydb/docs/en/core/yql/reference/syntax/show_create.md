# SHOW CREATE

`SHOW CREATE` returns a query required to recreate the structure of the specified object: a [table](../../../concepts/datamodel/table.md) or [view](../../../concepts/datamodel/view.md). The returned query may contain multiple DDL statements.

## Syntax

```yql
SHOW CREATE [TABLE|VIEW] <name>;
```

## Parameters

* `TABLE|VIEW` — The object type. Valid values are `TABLE` or `VIEW`.
* `<name>` — The object name. An absolute path may also be specified.

## Result

The command always returns **exactly one row** with three columns:

| Path            | PathType   | CreateQuery                      |
|-----------------|------------|----------------------------------|
| Path            | Table/View | DDL statements for creation      |

- **Path** — The path to the object (for example, `MyTable` or `MyView`).
- **PathType** — The type of object: `Table` or `View`.
- **CreateQuery** — The complete set of DDL statements needed to create the object:
    - For tables: the main [CREATE TABLE](create_table/index.md) statement (with the path relative to the database root), plus any additional statements describing the current configuration, such as:
        - [ALTER TABLE ... ALTER INDEX](alter_table/secondary_index#alter-index) — for index partitioning settings.
        - [ALTER TABLE ... ADD CHANGEFEED](alter_table/changefeed.md) — for adding a changefeed.
        - `ALTER SEQUENCE` — for restoring a `Sequence` state for `Serial` columns.
    - For views: the definition via [CREATE VIEW](create-view.md), and, if necessary, the statements the view has captured from the creation context, for example, [PRAGMA TablePathPrefix](pragma#table-path-prefix).


## Examples

### Row-oriented tables

```yql
SHOW CREATE TABLE my_table;
```

| Path            | PathType  | CreateQuery                     |
|-----------------|-----------|---------------------------------|
| `my_table`      | `Table`   | `CREATE TABLE...` — see below   |

```yql
CREATE TABLE `my_table` (
    `Key1` Uint32 NOT NULL,
    `Key2` Utf8 NOT NULL,
    `Key3` Serial4 NOT NULL,
    `Value1` Utf8 FAMILY `my_family`,
    `Value2` Bool,
    `Value3` String,
    INDEX `my_index` GLOBAL SYNC ON (`Key2`, `Value1`, `Value2`),
    FAMILY `my_family` (COMPRESSION = 'lz4'),
    PRIMARY KEY (`Key1`, `Key2`, `Key3`)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_PARTITION_SIZE_MB = 1000
);

ALTER TABLE `my_table`
    ADD CHANGEFEED `feed_3` WITH (MODE = 'KEYS_ONLY', FORMAT = 'JSON', RETENTION_PERIOD = INTERVAL('PT30M'), TOPIC_MIN_ACTIVE_PARTITIONS = 3)
;

ALTER SEQUENCE `/Root/my_table/_serial_column_Key3` START WITH 101 INCREMENT BY 404 RESTART;

ALTER TABLE `my_table`
    ALTER INDEX `my_index` SET (AUTO_PARTITIONING_BY_LOAD = ENABLED, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1000, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 5000)
;
```

### Column-oriented tables

```yql
SHOW CREATE TABLE my_table;
```

| Path            | PathType  | CreateQuery                     |
|-----------------|-----------|---------------------------------|
| `my_table`      | `Table`   | `CREATE TABLE...` — see below   |

```yql
CREATE TABLE `my_table` (
    `Key1` Uint64 NOT NULL,
    `Key2` Utf8 NOT NULL,
    `Key3` Int32 NOT NULL,
    `Value1` Utf8,
    `Value2` Int16,
    `Value3` String,
    FAMILY `default` (COMPRESSION = 'zstd'),
    FAMILY `Family1` (COMPRESSION = 'off'),
    FAMILY `Family2` (COMPRESSION = 'lz4'),
    PRIMARY KEY (`Key1`, `Key2`, `Key3`)
)
PARTITION BY HASH (`Key1`, `Key2`)
WITH (
    STORE = COLUMN,
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 100,
    TTL =
        INTERVAL('PT10S') TO EXTERNAL DATA SOURCE `/Root/tier1`,
        INTERVAL('PT1H') DELETE
    ON Key1 AS SECONDS
);
```

### Views

```yql
SHOW CREATE VIEW my_view;
```

| Path            | PathType  | CreateQuery                               |
|-----------------|-----------|-------------------------------------------|
| `my_view`       | `View`    | `PRAGMA TablePathPrefix...` — see below   |

```yql
PRAGMA TablePathPrefix = '/Root/DirA/DirB/DirC';

CREATE VIEW `my_view` WITH (security_invoker = TRUE) AS
SELECT
    *
FROM
    test_table
;
```