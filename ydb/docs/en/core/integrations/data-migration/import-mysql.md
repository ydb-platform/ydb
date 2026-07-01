# Importing table schemas and data from MySQL

## Introduction {#intro}

Migrating schemas and data from **MySQL** to {{ ydb-short-name }} — including **large tables** whose size exceeds available RAM — is performed using the [mysql2ydb utility](https://github.com/ydb-platform/mysql-ydb-importer).

**The utility's goal is to get an exact one-to-one copy of the source database.** Table and column names are preserved as in MySQL; data types are mapped to the nearest equivalents in {{ ydb-short-name }} (`INT` → `Int32`, `VARCHAR` → `Text`, `AUTO_INCREMENT` → `BigSerial`, etc.). Since names are transferred unchanged, they must be valid identifiers in {{ ydb-short-name }} (characters, length, reserved words). The result should be a {{ ydb-short-name }} database that replicates the original structure and allows queries with minimal "translation" from MySQL.

Each data fragment is read from MySQL by a single bounded query `SELECT`, written to {{ ydb-short-name }} in a batch (`BulkUpsert` or transactional `UPSERT`), and then freed from memory. The process repeats until the entire table is processed — no more than one batch is stored in memory at a time (no more than two with pipeline reading).

The import is performed in the following order:

1. The utility connects to MySQL and, based on the `information_schema` metadata, determines the list of tables and their structure.
2. Target tables with mapped data types are created in {{ ydb-short-name }}.
3. Data is transferred in batches: reading from MySQL and writing to {{ ydb-short-name }}.
4. After each batch is successfully written, the cursor position is saved to the state table — in case of failure, migration can be resumed from the last successful fragment.

## MySQL schema mapping {#schema-fidelity}

When transferring the schema, mysql2ydb aims for **structural correspondence**: the same table and column names, indexes where {{ ydb-short-name }} supports them, and types that preserve values without loss (unsigned integers remain unsigned, boolean types remain boolean, etc.).

The utility reads MySQL metadata from `information_schema` and generates {{ ydb-short-name }} DDL directly. The main type mappings are:

| MySQL type | Type in {{ ydb-short-name }} |
| --- | --- |
| `INT`, `MEDIUMINT`, `SMALLINT`, `TINYINT` | `Int32` |
| `BIGINT` | `Int64` |
| `INT UNSIGNED`, `MEDIUMINT UNSIGNED`, `SMALLINT UNSIGNED`, `TINYINT UNSIGNED` | `Uint32` |
| `BIGINT UNSIGNED` | `Uint64` |
| `TINYINT(1)` | `Bool` |
| `FLOAT` | `Float` |
| `DOUBLE`, `REAL` | `Double` |
| `BIT` | `Uint64` (including `BIT(1)` — wrapped in `Uint64`, not in `Bool`) |
| `CHAR`, `VARCHAR`, `TEXT`, `MEDIUMTEXT`, `LONGTEXT` | `Text` |
| `ENUM`, `SET`, `JSON` | `Text` |
| `BINARY`, `VARBINARY`, `BLOB`, `MEDIUMBLOB`, `LONGBLOB` | `String` (bytes inline in the same column) |
| `DATE` | `Date` |
| `DATETIME`, `TIMESTAMP` | `Timestamp` — the value is stored as UTC. For `DATETIME` (a type without time zone), the interpretation depends on the `time_zone` MySQL setting during reading; keep this in mind when migrating historical data. |
| `DECIMAL(p, s)`, `NUMERIC(p, s)` | `Decimal(22, 9)` for all — the original `p`/`s` are not preserved; values that do not fit in 22 digits will be lost |
| `YEAR` | `Uint16` |
| Other types | `Text` (fallback) |

### Structural decisions {#schema-structure}

| MySQL schema element | mysql2ydb decision |
| --- | --- |
| Table and column names | As in MySQL (`users`, `orders`, …); must be valid {{ ydb-short-name }} identifiers |
| `AUTO_INCREMENT` | `BigSerial` + `ALTER SEQUENCE … START WITH` from `TABLES.AUTO_INCREMENT` |
| Secondary `KEY` / `UNIQUE KEY` | `INDEX … GLOBAL ASYNC` / `GLOBAL UNIQUE SYNC` in `CREATE TABLE` |
| Table without `PRIMARY KEY` | Not supported when automatically creating the schema — see [Tables without a primary key](#tables-without-pk) |
| Partitioning | Only `AUTO_PARTITIONING_BY_LOAD` |

### Secondary indexes {#secondary-indexes}

Non-unique MySQL keys become `INDEX … GLOBAL ASYNC`, unique ones (except the primary key) become `INDEX … GLOBAL UNIQUE SYNC`. Tables with synchronous unique indexes automatically switch to transactional `UPSERT` during data loading, because `BulkUpsert` only supports asynchronous indexes.

### Examples {#schema-examples}

**`AUTO_INCREMENT`** — the MySQL column `id BIGINT AUTO_INCREMENT` becomes:


```sql
`id` BigSerial NOT NULL,
PRIMARY KEY (`id`)
```


After `CREATE TABLE`, if the value of `information_schema.TABLES.AUTO_INCREMENT` is known:


```sql
ALTER SEQUENCE `<db>/<table>/_serial_column_id` START WITH <next_value> RESTART
```


### Tables without a primary key {#tables-without-pk}

In {{ ydb-short-name }}, each table must have a `PRIMARY KEY`. mysql2ydb takes the key **only** from MySQL columns with `COLUMN_KEY = 'PRI'`; `UNIQUE KEY` indexes become secondary `GLOBAL UNIQUE SYNC`, but are **not** substituted for the primary key. If a table has no `PRIMARY KEY`, schema creation fails with an error (`PRIMARY KEY ()` — invalid DDL), and data transfer does not start.

Before migration, you need to set a key in MySQL for such tables, for example:


```sql
ALTER TABLE my_table ADD COLUMN id BIGINT AUTO_INCREMENT PRIMARY KEY FIRST;
```


or assign `PRIMARY KEY` to an existing column (or set of columns) if it is truly unique.

If you cannot add `PRIMARY KEY` in MySQL, but you can prepare the schema in {{ ydb-short-name }} manually, create the table with the required key yourself and run the utility with the `-data-only` flag. Reading from MySQL without `PRIMARY KEY` in the metadata will go through `LIMIT/OFFSET`: on each page, MySQL rescans all previous rows, so migration time grows quadratically with table size — for large tables, it is better to add a key in MySQL beforehand. The key columns in {{ ydb-short-name }} must be present among the MySQL columns.

If you need automatic import without a key in MySQL, consider [YDB Importer](#ydb-importer-comparison) — it adds a synthetic column `ydb_synth_key` (SHA-256 of the row). mysql2ydb does not create synthetic keys.

## Efficient access to MySQL {#efficient-access}

The utility minimizes accesses to MySQL and limits the size of each query:

1. **Page-by-page reading by primary key** — `WHERE (pk…) > (?) ORDER BY pk LIMIT n`. Without full table scan into memory.
2. **Read → write pipeline** — one goroutine reads the next batch from MySQL while another writes the previous one to {{ ydb-short-name }}. No more than two batches are in flight at the same time per table.
3. **Adaptive batch size** — the number of rows is limited by available RAM and the average row size from `information_schema`.
4. **Progress checkpoint** — after each successfully written batch, the cursor position and row counter are saved in the {{ ydb-short-name }} service table; a restart continues from the last batch.


```text
MySQL: SELECT batch N   →  BulkUpsert(batch N)   →  YDB
MySQL: SELECT batch N+1 →  BulkUpsert(batch N+1) →  YDB
...
```


## Features {#features}

- **One-to-one schema copy** — original table and column names; mapping of MySQL types to the nearest {{ ydb-short-name }} types (`AUTO_INCREMENT` → `BigSerial`, `UNSIGNED` → `Uint32`/`Uint64`, secondary indexes, `TINYINT(1)` → `Bool`; see the [MySQL schema mapping](#schema-fidelity) section).
- **Schema creation** — tables in {{ ydb-short-name }} are created from MySQL metadata (`information_schema`).
- **Fragment-by-fragment data migration**: the table is processed in fixed-size chunks; the data volume may exceed available RAM.
- **Upsert by key** — data is written via `BulkUpsert` or transactional `UPSERT`; re-running does not create duplicate rows.
- **Resumption** — progress for each table is stored in {{ ydb-short-name }}; an interrupted migration can be resumed from the last batch.

## Installation {#install}

Source code and build instructions are in the [mysql-ydb-importer repository](https://github.com/ydb-platform/mysql-ydb-importer) on GitHub.

Building requires [Go](https://go.dev/) version not lower than specified in `go.mod` of the repository:


```bash
git clone https://github.com/ydb-platform/mysql-ydb-importer.git
cd mysql-ydb-importer
go build -o mysql2ydb ./cmd/mysql2ydb
```


## Usage procedure {#use}

MySQL connection parameters are read by default from the **~/.my.cnf** file (like the `mysql` client), section `[client]`. The `-mysql` flag overrides the configuration file.

{% note warning "Tables without primary key" %}

Tables without `PRIMARY KEY` in MySQL **are not supported** during automatic schema creation. Before running, add a primary key in MySQL or prepare the schema manually and use `-data-only` (see the section [Tables without a primary key](#tables-without-pk)).

{% endnote %}

Example `~/.my.cnf`:


```ini
[client]
user = myuser
password = mypass
host = localhost
port = 3306
database = mydb
```


If the server requires a secure connection (`require_secure_transport=ON`), add to `[client]`:


```ini
ssl-mode = REQUIRED
```


For a self-signed certificate (without verification):


```ini
ssl-mode = REQUIRED
ssl-verify = 0
```


Alternative option — `ssl=1` and `ssl-verify=0`.

Minimum startup when `~/.my.cnf` is present (only `-ydb` is required):


```bash
./mysql2ydb -ydb "grpc://localhost:2136"
```


With explicit MySQL DSN (ignores `~/.my.cnf`):


```bash
./mysql2ydb -mysql "user:$MYSQL_PASSWORD@tcp(localhost:3306)/mydb" -ydb "grpc://localhost:2136" -batch-size 10000
```


### Command line flags {#flags}

| Flag | Description |
| --- | --- |
| `-mysql` | DSN MySQL (if specified, overrides `~/.my.cnf`) |
| `-ydb` | Endpoint {{ ydb-short-name }} (required) |
| `-ydb-database` | Path to {{ ydb-short-name }} database (default `local`) |
| `-schema-only` | Only create the schema, without data transfer |
| `-data-only` | Only transfer data (the schema in {{ ydb-short-name }} must already exist — including one created manually) |
| `-batch-size` | Target batch size in rows (default 10,000) |
| `-max-chunk-rows` | Hard upper limit of rows in a batch (default 25,000) |
| `-parallel-tables` | Number of tables for parallel transfer (default 1) |
| `-tables` | Comma-separated list of tables (default – all) |
| `-force` | Migrate all tables from scratch, ignoring the saved state |
| `-force-recreate` | Delete all tables in {{ ydb-short-name }} and recreate the schema from scratch |

### Examples {#examples}

Schema only:


```bash
./mysql2ydb -mysql "..." -ydb "grpc://localhost:2136" -schema-only
```


Only data (after schema creation):


```bash
./mysql2ydb -mysql "..." -ydb "grpc://localhost:2136" -data-only -batch-size 5000
```


Selected tables:


```bash
./mysql2ydb -mysql "..." -ydb "grpc://localhost:2136" -tables "users,orders"
```


## Fragment-based reading {#chunked-reading}

- For tables with a **primary key** in MySQL, cursor-based pagination is used: `WHERE (pk > ?) ORDER BY pk LIMIT batch_size`. No more than one batch is kept in memory at a time.
- If there is no `PRIMARY KEY` in MySQL, but the schema in {{ ydb-short-name }} has already been created manually (mode `-data-only`), `LIMIT batch_size OFFSET offset` is applied — see the section [Tables without a primary key](#tables-without-pk).

## Writing to {{ ydb-short-name }} {#bulkupsert}

By default, data is written via `BulkUpsert`. For tables with synchronous unique indexes, the utility automatically switches to transactional `UPSERT`, since `BulkUpsert` only supports asynchronous indexes.

Queries to {{ ydb-short-name }} are marked as idempotent, so the SDK automatically retries them on network failures. The absence of duplicates when restarting the migration is ensured by overwriting rows by key and saving progress in a service table (see the [Features](#features) section, the "Resumption" item).

## Comparison with YDB Importer {#ydb-importer-comparison}

There are other tools for loading data into {{ ydb-short-name }}. [YDB Importer](import-jdbc.md) is a universal JDBC importer for PostgreSQL, Oracle, SQL Server, and other sources: XML configuration, parallel import, partitioning configuration.

**mysql2ydb** solves a more specific task — one-to-one migration of a MySQL database using a single binary file, without JVM. For migration only from MySQL with large tables, `~/.my.cnf` and the `-ydb` flag are sufficient; progress is saved in the service table {{ ydb-short-name }}.

|  | [YDB Importer](import-jdbc.md) | **mysql2ydb** |
| --- | --- | --- |
| Runtime | Java + JDBC drivers + XML configuration | One binary file in Go, native MySQL protocol |
| Scope | Multiple JDBC sources | Only MySQL |
| Default batch size | 1,000 rows (`max-batch-rows`) | 10,000 rows (`-batch-size`), with auto-tuning |
| Large tables | Parallel ranges, partition buffers | Cursor-based paginated reading, batch size based on RAM |
| Resumption after failure | Re-running the import | Checkpoint per batch in the service table {{ ydb-short-name }} |
| Setting | JDBC JAR files, XML configuration | `~/.my.cnf` and two flags |

YDB Importer is designed for importing from many JDBC sources into a **configurable** {{ ydb-short-name }} structure (`table-name-format`, `blob-name-format`, date conversion modes, optional synthetic keys). Below are the practical differences when importing the same MySQL database with two tools:

| MySQL feature | mysql2ydb (one-to-one copy) | ydb-importer |
| --- | --- | --- |
| **Table names** | As in MySQL (`users`, `orders`, …) | Rename by pattern, e.g., `mysql1/${schema}/${table}` → `mysql1/mydb/users` ( [configuration example](https://github.com/ydb-platform/ydb-importer/blob/main/scripts/sample-mysql.xml)) |
| **Column names** | As in MySQL | Sanitization: spaces, `.`, `/`, `` ` `` → `_` |
| `AUTO_INCREMENT` | `BigSerial` + `ALTER SEQUENCE … START WITH` from `TABLES.AUTO_INCREMENT` | Regular `Int32`/`Int64`; no `BigSerial` or sequence reset |
| `INT UNSIGNED`, `BIGINT UNSIGNED`, … | `Uint32` / `Uint64` | JDBC maps integers to signed `Int32`/`Int64` |
| `TINYINT(1)` | `Bool` | `TINYINT` → `Int32` (`BOOLEAN` → `Bool`) |
| `BIT` | `Uint64` | `Bool` (JDBC `Types.BIT`) |
| Secondary `KEY` / `UNIQUE KEY` | `INDEX … GLOBAL ASYNC` / `GLOBAL UNIQUE SYNC` in `CREATE TABLE` | Only `PRIMARY KEY` in DDL; secondary indexes are not created |
| `ENUM`, `SET`, `JSON` | `Text` | `Text` (similar) |
| `BLOB` / `BINARY` | Same column, value inline in `String` (bytes) | Column becomes `Int64` (blob identifier); data is moved to a **separate table** `${schema}/${table}_${field}` with `(id, pos, val)` rows in 64 KB blocks |
| `TEXT` / `CLOB` (large text) | Inline `Text` in the main table | Optionally a separate CLOB table (32 K character blocks) or inline `Text` depending on settings |
| Table **without a primary key** | **Not supported** — `CREATE TABLE` fails with an error; add `PRIMARY KEY` in MySQL before migration | A column `ydb_synth_key Text` is added as PK (SHA-256 of the row); **duplicates are collapsed** into one row |
| `DATE` | `Date` | `Date32` by default (`conv-date=DATE_NEW`) |
| `DATETIME` / `TIMESTAMP` | `Timestamp` | `Datetime64` / `Timestamp64` by default |
| `TIME` | `Text` (fallback) | `Int32` (seconds since midnight) |
| `DECIMAL(p,s)` | `Decimal(22, 9)` for all | `Decimal(p,s)` when `allow-custom-decimal=true` |
| `YEAR` | `Uint16` | Not covered in MySQL type tests |
| Partitioning | Only `AUTO_PARTITIONING_BY_LOAD` | `PARTITION_AT_KEYS`, splitting by source partitions, `HASH` for column store, etc. |

### Working with BLOBs {#blob-handling}

MySQL table `attachments (id INT, data MEDIUMBLOB)`:

|  | Column `data` in the main table | Where bytes are stored |
| --- | --- | --- |
| **mysql2ydb** | `data String` | In the same row |
| **ydb-importer** | `data Int64` (reference) | Separate table `…/attachments_data` with chunked rows |

With mysql2ydb, you can perform `SELECT data FROM attachments WHERE id = 1` the same way as in MySQL. With ydb-importer, you need to join the main table with an additional blob table.
