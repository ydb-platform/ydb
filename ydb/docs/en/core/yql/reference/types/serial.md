
# Serial data types

Serial data types are integers with an additional value-generation mechanism. They are used for auto-increment columns: each new row inserted into the table automatically gets a unique value in such a column (similar to the [SERIAL](https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-SERIAL) type in PostgreSQL or [AUTO_INCREMENT](https://dev.mysql.com/doc/refman/9.0/en/example-auto-increment.html) in MySQL).

{% note info %}

Using serial types as a primary key is not recommended: monotonically increasing values lead to uneven data distribution and hot partitions. For details, see [{#T}](../../../dev/primary-key/row-oriented.md).

{% endnote %}

## Usage example

``` yql
CREATE TABLE users (
    user_id Serial,
    name Utf8,
    email Utf8,
    PRIMARY KEY (user_id)
);
```

``` yql
UPSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com');
INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com');
REPLACE INTO users (name, email) VALUES ('John', 'john@example.com');
```

``` yql
SELECT * FROM users;
```

| email               | name  | user_id |
|---------------------|-------|---------|
| `alice@example.com` | Alice | 1       |
| `bob@example.com`   | Bob   | 2       |
|  `john@example.com` | John  | 3       |

You can supply a value for a `Serial` column explicitly on insert; the row is then handled like a plain integer column, and the `Sequence` is not affected:

``` yql
UPSERT INTO users (user_id, name, email) VALUES (4, 'Peter', 'peter@example.com');
```

## Description

Only columns that participate in a table's primary key may use the `Serial` type.

Defining this type on a column creates a separate schema object, `Sequence`, bound to that column and used as a generator of sequence values. This object is private and hidden from the user. The `Sequence` is destroyed together with the table.

Sequence values start at 1, increment by 1, and are bounded according to the type used.

| Type           | Maximum value | Value type |
|----------------|---------------|------------|
| `SmallSerial`  | $2^{15}–1$    | `Int16`    |
| `Serial2`      | $2^{15}–1$    | `Int16`    |
| `Serial`       | $2^{31}–1$    | `Int32`    |
| `Serial4`      | $2^{31}–1$    | `Int32`    |
| `Serial8`      | $2^{63}–1$    | `Int64`    |
|  `BigSerial`   | $2^{63}–1$    | `Int64`    |

If the sequence overflows on insert, an error is returned:

```text
Error: Failed to get next val for sequence: /dev/test/users/_serial_column_user_id, status: SCHEME_ERROR
    <main>: Error: sequence [OwnerId: <some>, LocalPathId: <some>] doesn't have any more values available, code: 200503
```

The next value is allocated by the generator before the row is actually inserted and is considered consumed even if the row is not successfully written (for example, when the transaction rolls back). Therefore the set of values in such a column may have gaps and consist of several disjoint ranges.

For tables with auto-increment columns, the [copy](../../../reference/ydb-cli/tools-copy.md), [dump](../../../reference/ydb-cli/export-import/tools-dump.md), [restore](../../../reference/ydb-cli/export-import/import-file.md), and [import](../../../reference/ydb-cli/export-import/import-s3.md)/[export](../../../reference/ydb-cli/export-import/export-s3.md) operations are supported.

