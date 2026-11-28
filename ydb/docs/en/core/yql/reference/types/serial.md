# Serial data types

Serial data types are integers with automatic value generation. If a column value is omitted when adding a row, {{ ydb-short-name }} obtains it from the [sequence](../../../concepts/datamodel/sequence.md) associated with that column.

Serial types are supported for both key and non-key columns in [row-oriented tables](../../../concepts/datamodel/table.md#row-oriented-tables). Column-oriented tables do not support this mechanism.

## Types and value ranges {#types-and-value-ranges}

A sequence is created automatically for each serial column when you [create a table](../syntax/create_table/index.md). By default, generation starts at 1 with an increment of 1. You can change the start value, increment, and generator position using [ALTER SEQUENCE](../syntax/alter-sequence.md).

| Type          | Maximum value | Value type |
|---------------|---------------|------------|
| `SmallSerial` | $2^{15}-1$    | `Int16`    |
| `Serial2`     | $2^{15}-1$    | `Int16`    |
| `Serial`      | $2^{31}-1$    | `Int32`    |
| `Serial4`     | $2^{31}-1$    | `Int32`    |
| `Serial8`     | $2^{63}-1$    | `Int64`    |
| `BigSerial`   | $2^{63}-1$    | `Int64`    |

If the sequence has no more values available, a write that requires an automatically generated value fails:

```text
Error: Failed to get next val for sequence: /dev/test/users/_serial_column_user_id, status: SCHEME_ERROR
    <main>: Error: sequence [OwnerId: <some>, LocalPathId: <some>] doesn't have any more values available, code: 200503
```

The generator allocates a value before the row is written. A failed write or transaction rollback does not return the allocated value to the generator, so the column can have gaps. For details on allocation and table operations, see [{#T}](../../../concepts/datamodel/sequence.md).

## Changing columns

A serial column automatically has the `NOT NULL` constraint. The current [ALTER TABLE](../syntax/alter_table/columns.md) restrictions for these columns are:

* You cannot add a serial column to an existing table using `ADD COLUMN`. Specify the serial type when creating the table.
* You cannot change nullability or the column family (`FAMILY`), or set or drop a default value (`DEFAULT`). Change the generator parameters separately using [ALTER SEQUENCE](../syntax/alter-sequence.md).
* You can drop a non-key serial column using `DROP COLUMN`. Its sequence is dropped with it. You cannot drop a column that is part of the primary key, just as with other key columns.

## Usage example {#usage-example}

{% note info %}

A monotonically increasing primary key can distribute load unevenly across partitions. For recommendations on using serial columns in a key, see [{#T}](../../../dev/primary-key/row-oriented.md#monotonic-serial).

{% endnote %}

Create a users table with a composite primary key. The application calculates `user_hash`, for example, by hashing the `email` address. The example uses illustrative hash values.

```yql
CREATE TABLE users (
    user_hash Uint64,
    user_id Serial,
    name Utf8,
    email Utf8,
    PRIMARY KEY (user_hash, user_id)
);
```

When you write using [UPSERT](../syntax/upsert_into.md), [INSERT](../syntax/insert_into.md), or [REPLACE](../syntax/replace_into.md) without specifying `user_id`, this column's value is generated automatically:

```yql
UPSERT INTO users (user_hash, name, email) VALUES (123456789, 'Alice', 'alice@example.com');
INSERT INTO users (user_hash, name, email) VALUES (987654321, 'Bob', 'bob@example.com');
REPLACE INTO users (user_hash, name, email) VALUES (111111111, 'John', 'john@example.com');
```

Read the rows using [SELECT](../syntax/select/index.md):

```yql
SELECT * FROM users ORDER BY user_id;
```

If the statements run sequentially on a new table with no other requests to the generator, the result is:

| user_hash | email               | name  | user_id |
|-----------|---------------------|-------|---------|
| 123456789 | `alice@example.com` | Alice | 1       |
| 987654321 | `bob@example.com`   | Bob   | 2       |
| 111111111 | `john@example.com`  | John  | 3       |

## Explicit values {#explicit-values}

You can specify a serial column's value explicitly, for example, when restoring data:

```yql
UPSERT INTO users (user_hash, user_id, name, email) VALUES (222222222, 10, 'Peter', 'peter@example.com');
```

This write is handled like a regular integer value and does not change the sequence position. The generator does not check or skip values already in the table. As a result, subsequent automatic generation or `ALTER SEQUENCE RESTART` can produce a value that has already been written.

For a non-key serial column, equal values do not by themselves violate primary key uniqueness. If the serial column is part of the key, a conflict depends on the **entire** primary key matching: `INSERT` fails, `UPSERT` updates the existing row, and `REPLACE` replaces it. When mixing explicit and automatically generated values, align the sequence position with existing data using [ALTER SEQUENCE](../syntax/alter-sequence.md).
