# CHECKSUM TABLE

`CHECKSUM TABLE` calculates a checksum: a short string that represents the data in a table. Use it to check whether a copy or restored table has the same contents as the original.

To calculate the checksum of `my_table`, run:

```yql
CHECKSUM TABLE my_table;
```

The command reads every row and returns the checksum and row count. All user columns, including primary key columns, contribute to the checksum. System columns are ignored.

## Syntax

The statement has the following syntax. Square brackets mark an optional clause:

```yql
CHECKSUM TABLE <table_path> [WITH (ALGORITHM = "<algorithm>")];
```

Where:

- `table_path` is the table name or path. Paths follow the same rules as in [`SELECT`](select/index.md), including [`PRAGMA TablePathPrefix`](pragma.md#table-path-prefix).
- `algorithm` is the checksum algorithm. Write its name directly in quotes, with the capitalization shown below. If the `WITH` clause is omitted, the algorithm is `CityHash64`.

| Algorithm | Checksum length |
| --- | --- |
| `CityHash64` | 16 characters |
| `CityHash128` | 32 characters |
| `Sha256` | 64 characters |


For example, to use `Sha256` with an absolute table path, enclose the path in backticks:

```yql
CHECKSUM TABLE `/Root/my_database/my_table` WITH (ALGORITHM = "Sha256");
```

## Result {#result}

The statement returns exactly one row with the following columns:

| Column | Type | Description |
| --- | --- | --- |
| `algorithm` | `Utf8` | Algorithm used to calculate the checksum. |
| `count` | `Uint64` | Number of rows in the table. |
| `hash` | `String` | Checksum as uppercase hexadecimal text, in the format returned by [`String::HexEncode`](../udf/list/string.md). |
| `version` | `Uint32` | Checksum format version, currently `1`. |

For an empty table, `count` is `0` and the checksum contains only `0` characters. Its length still depends on the algorithm. With the default algorithm, the result is:

| algorithm | count | hash | version |
| --- | --- | --- | --- |
| `CityHash64` | `0` | `0000000000000000` | `1` |

## Before running

- You need the same table read permissions as for `SELECT`.
- The command reads the entire table, so checking a large table can take time and add load to the database.

To check selected rows or columns, or get a separate checksum for each group, use the [`DigestAggregate` examples](../udf/list/digest-aggregate.md#examples).
