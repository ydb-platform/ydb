# CHECK TABLE INDEX

`CHECK TABLE INDEX` checks whether an index agrees with its base table. It compares the expected and stored index entries at the same snapshot and reports missing entries, extra entries, and mismatched values.

## Syntax

Specify the table path and the name of the index to check.

```yql
CHECK TABLE <table_path> INDEX <index_name>;
```

- `table_path` is the table name or path and follows the same rules as in [`SELECT`](select/index.md).
- `index_name` is the name of an index on that table.

## Requirements

- The index must be ready. Supported types on row-oriented tables are synchronous [secondary indexes](create_table/secondary_index.md), including unique indexes, and [JSON indexes](create_table/json_index.md) with non-compact storage.
- The request must run as one execution block and return a single result set, produced by `CHECK TABLE INDEX`.
- An explicit transaction must use [Snapshot Read-Only](../../../concepts/transactions.md#snapshot-read-only). If you omit transaction control, the statement runs in an implicit Snapshot Read-Only transaction.
- The check fails if the snapshot becomes unavailable, the index is replaced during validation, or multiple base rows share the same stored document ID.
- Other index types and storage formats return `UNSUPPORTED` with the diagnostic `Index validation is not implemented for <type>`.
- Use Query Service, including `ExecuteScript`. The legacy `ExecuteYqlScript` and `StreamExecuteYqlScript` methods return `UNSUPPORTED`.

## Result {#result}

The statement returns one row with the following columns.

| Column | Type | Description |
| --- | --- | --- |
| `SnapshotStep` | `Uint64` | Step of the snapshot used to read the table and index. |
| `SnapshotTxId` | `Uint64` | Transaction ID component of the same snapshot. |
| `Status` | `String` | `OK` if the check found no discrepancies, or `INCONSISTENT` otherwise. |
| `Missing` | `Uint64` | Number of expected entries missing from the index. |
| `Extra` | `Uint64` | Number of stored index entries with no corresponding expected entry. |
| `ValueMismatch` | `Uint64` | Number of entries with a matching key and different stored values, such as values in `COVER` columns. |
| `EmptyTokenPostings` | `Uint64` | Number of stored entries with an empty token in a prefixed JSON index. These entries also count toward `Extra`. |
| `Sample` | List of structs | Up to 100 discrepancies. Each sample entry can occupy up to 8 KiB when encoded. The counts include all discrepancies. |

Each sampled discrepancy includes `Kind`, `Role`, `Reason`, and the fields `Key`, `Expected`, and `Actual`. `Key` has type `List<Struct<Name:String,Type:String,Value:String?>>`. `Expected` and `Actual` are optional lists of the same type and are absent when the corresponding entry is missing. Each list item gives the column name, its type, and its value as a `String`. A `NULL` column value leaves `Value` absent.

If the row-ID mapping is inconsistent, the sample omits extra JSON postings that require that mapping to identify. The counts still include those postings.

## Example

To check the `idx` index on the `Docs` table, run the following statement.

```yql
CHECK TABLE Docs INDEX idx;
```

Suppose the index entry for `Tag = 10` and `Id = 1` stores `NULL` in the covered `Data` column, while the table stores `original`. The statement reports one value mismatch.

| SnapshotStep | SnapshotTxId | Status | Missing | Extra | ValueMismatch | EmptyTokenPostings | Sample |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `120` | `42` | `INCONSISTENT` | `0` | `0` | `1` | `0` | One `VALUE_MISMATCH` entry for `Tag = 10`, `Id = 1`. |

The sample's `Key` has items for `Tag` and `Id` with `Value` set to `"10"` and `"1"`. Both `Expected` and `Actual` contain an item named `Data`. Its `Value` is `"original"` in `Expected` and absent in `Actual`.
