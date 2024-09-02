# Data schema versioning and migration in YDB using "goose"

## Introduction

[Goose](https://github.com/pressly/goose) is an open-source tool that helps to version the data schema in the database and manage migrations between these versions. Goose supports many different database management systems, including YDB. Goose uses migration files and stores the state of migrations directly in the database in a special table.

## Install goose

Goose installation options are described in [its documentation](https://github.com/pressly/goose/blob/master/README.md#install).

## Launch arguments goose

After installation, the `goose` command line utility can be called:

```
$ goose <DB> <CONNECTION_STRING> <COMMAND> <COMMAND_ARGUMENTS>
```

Where:
- `<DB>` - database engine, for YDB you should write `goose ydb`
- `<CONNECTION_STRING>` - database connection string.
- `<COMMAND>` - the command to be executed. A complete list of commands is available in the built-in help (`goose help`).
- `<COMMAND_ARGUMENTS>` - command arguments.

## YDB connection string

To connect to YDB you should use a connection string like:

```
<protocol>://<host>:<port>/<database_path>?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric
```

Where:
- `<protocol>` - connection protocol (`grpc` for an unsecured connection or `grpcs` for a secure [TLS](https://en.wikipedia.org/wiki/Transport_Layer_Security) connection). The secure connection requires certificates. You should declare certificates like this: `export YDB_SSL_ROOT_CERTIFICATES_FILE=/path/to/ydb/certs/CA.pem`.
- `<host>` - hostname for connecting to YDB cluster.
- `<port>` - port for connecting to YDB cluster.
- `<database_path>` - database in the YDB cluster.
- `go_query_mode=scripting` - special `scripting` mode for executing queries by default in the YDB driver. In this mode, all requests from goose are sent to the YDB `scripting` service, which allows processing of both [DDL](https://en.wikipedia.org/wiki/Data_definition_language) and [DML](https://en.wikipedia.org/wiki/Data_manipulation_language) SQL statements.
- `go_fake_tx=scripting` - support for transaction emulation in query execution mode through the YDB `scripting` service. The fact is that in YDB, executing `DDL` `SQL` statements in a transaction is impossible (or incurs significant overhead). In particular, the `scripting` service does not allow interactive transactions (with explicit `Begin`+`Commit`/`Rollback`). Accordingly, the transaction emulation mode does not actually do anything (`noop`) on the `Begin`+`Commit`/`Rollback` calls from `goose`. This trick can, in rare cases, cause an individual migration step to end up in an intermediate state. The YDB team is working on a new `query` service that should  eliminate this risk.
- `go_query_bind=declare,numeric` - support for bindings of auto-inference of YQL types from query parameters (`declare`) and support for bindings of numbered parameters (`numeric`). YQL is a strongly typed language that requires you to explicitly specify the types of query parameters in the body of the `SQL` query itself using the special `DECLARE` statement. Also, YQL only supports named query parameters (for example, `$my_arg`), while the goose core generates SQL queries with numbered parameters (`$1`, `$2`, etc.) . The `declare` and `numeric` bindings modify the original queries from `goose` at the YDB driver level.

If connecting to a local YDB docker container, the connection string could look like:

```
grpc://localhost:2136/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric
```

Let's store this connection string to an environment variable to re-use it later:

```
export YDB_CONNECTION_STRING="grpc://localhost:2136/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric"
```

Further examples of calling `goose` commands will contain exactly this connection string.

## Directory with migration files

Let's create a migrations directory and then all `goose` commands should be executed in this directory:

```
$ mkdir migrations && cd migrations
```

## Managing migrations using goose

### Creating migration files and applying to database

The migration file can be generated using the `goose create` command:

```
$ goose ydb $YDB_CONNECTION_STRING create 00001_create_first_table sql
2024/01/12 11:52:29 Created new file: 20240112115229_00001_create_first_table.sql
```

This means that the tool has created a new migration file `<timestamp>_00001_create_table_users.sql`:
```
$ ls
20231215052248_00001_create_table_users.sql
```

After executing the `goose create` command, a migration file `<timestamp>_00001_create_table_users.sql` will be created with the following content :

```
-- +goose Up
-- +goose StatementBegin
SELECT 'up SQL query';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 'down SQL query';
-- +goose StatementEnd
```

This migration file structure helps keep the instructions that lead to the next version of the database in context. It is also easy, without unnecessary distractions, to write instructions that roll back a database change.

The migration file consists of two sections:

1. `+goose Up` is an area where we can record the migration steps.
2. `+goose Down` is an area where we can write queries to revert changes of the `+goose Up` steps.

Goose carefully inserted placeholder queries:

```
SELECT 'up SQL query';
```

And

```
SELECT 'down SQL query';
```

So that we can replace them with the real migration SQL queries for change the schema for the YDB database, which is accessible through the corresponding connection string.

Let's edit the migration file `<timestamp>_00001_create_table_users.sql` so that when applying the migration we create a table with necessary schema, and when rolling back the migration we delete the created table:

```
-- +goose Up
-- +goose StatementBegin
CREATE TABLE users (
     id Uint64,
     username Text,
     created_at Timestamp,
     PRIMARY KEY (id)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE users;
-- +goose StatementEnd
```

We can check status of migrations:

```
$ goose ydb $YDB_CONNECTION_STRING status
2024/01/12 11:53:50     Applied At                  Migration
2024/01/12 11:53:50     =======================================
2024/01/12 11:53:50     Pending                  -- 20240112115229_00001_create_first_table.sql
```

Status `Pending` of migration means that migration has not been applied yet. You can apply such migrations with commands `goose up` or `goose up-by-one`.

Let's apply migration using `goose up`:
```
$ goose ydb $YDB_CONNECTION_STRING up
2024/01/12 11:55:18 OK   20240112115229_00001_create_first_table.sql (93.58ms)
2024/01/12 11:55:18 goose: successfully migrated database to version: 20240112115229
```

Let's check the status of migration through `goose status`:
```
$ goose ydb $YDB_CONNECTION_STRING status
2024/01/12 11:56:00     Applied At                  Migration
2024/01/12 11:56:00     =======================================
2024/01/12 11:56:00     Fri Jan 12 11:55:18 2024 -- 20240112115229_00001_create_first_table.sql
```

Status `Pending` changed to timestamp `Fri Jan 12 11:55:18 2024` - this means that migration applied.

There are alternative options to see the applied changes:

{% list tabs %}

- Using YDB UI on http://localhost:8765

  ![YDB UI after the first migration](../_assets/goose-ydb-ui-after-first-migration.png =450x)

- Using YDB CLI

  ```
  $ ydb -e grpc://localhost:2136 -d /local scheme describe users
  <table> users

  Columns:
  ┌────────────┬────────────┬────────┬─────┐
  │ Name       │ Type       │ Family │ Key │
  ├────────────┼────────────┼────────┼─────┤
  │ id         │ Uint64?    │        │ K0  │
  │ username   │ Utf8?      │        │     │
  │ created_at │ Timestamp? │        │     │
  └────────────┴────────────┴────────┴─────┘

  Storage settings:
  Store large values in "external blobs": false

  Column families:
  ┌─────────┬──────┬─────────────┬────────────────┐
  │ Name    │ Data │ Compression │ Keep in memory │
  ├─────────┼──────┼─────────────┼────────────────┤
  │ default │      │ None        │                │
  └─────────┴──────┴─────────────┴────────────────┘

  Auto partitioning settings:
  Partitioning by size: true
  Partitioning by load: false
  Preferred partition size (Mb): 2048
  Min partitions count: 1
  ```

{% endlist %}

Let's make the second migration that adds a column `password_hash` to the `users` table:

```
$ goose ydb $YDB_CONNECTION_STRING create 00002_add_column_password_hash_into_table_users sql
2024/01/12 12:00:57 Created new file: 20240112120057_00002_add_column_password_hash_into_table_users.sql
```

Let's edit the migration file `<timestamp>_00002_add_column_password_hash_into_table_users.sql`:

```
-- +goose Up
-- +goose StatementBegin
ALTER TABLE users ADD COLUMN password_hash Text;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE users DROP COLUMN password_hash;
-- +goose StatementEnd
```

We can check the migration statuses again:

```
$ goose ydb $YDB_CONNECTION_STRING status
2024/01/12 12:02:40     Applied At                  Migration
2024/01/12 12:02:40     =======================================
2024/01/12 12:02:40     Fri Jan 12 11:55:18 2024 -- 20240112115229_00001_create_first_table.sql
2024/01/12 12:02:40     Pending                  -- 20240112120057_00002_add_column_password_hash_into_table_users.sql
```

Now we see the first migration in applied status and the second in pending status.

Let's apply the second migration using `goose up-by-one`:
```
$ goose ydb $YDB_CONNECTION_STRING up-by-one
2024/01/12 12:04:56 OK   20240112120057_00002_add_column_password_hash_into_table_users.sql (59.93ms)
```

Let's check the migration status through `goose status`:
```
$ goose ydb $YDB_CONNECTION_STRING status
2024/01/12 12:05:17     Applied At                  Migration
2024/01/12 12:05:17     =======================================
2024/01/12 12:05:17     Fri Jan 12 11:55:18 2024 -- 20240112115229_00001_create_first_table.sql
2024/01/12 12:05:17     Fri Jan 12 12:04:56 2024 -- 20240112120057_00002_add_column_password_hash_into_table_users.sql
```

Both migration are fully applied.

Let's use the same methods to see the new changes:

{% list tabs %}

- Using YDB UI on http://localhost:8765

  ![YDB UI after apply second migration](../_assets/goose-ydb-ui-after-second-migration.png =450x)

- Using YDB CLI

  ```
  $ ydb -e grpc://localhost:2136 -d /local scheme describe users
  <table> users

  Columns:
  ┌───────────────┬────────────┬────────┬─────┐
  │ Name          │ Type       │ Family │ Key │
  ├───────────────┼────────────┼────────┼─────┤
  │ id            │ Uint64?    │        │ K0  │
  │ username      │ Utf8?      │        │     │
  │ created_at    │ Timestamp? │        │     │
  │ password_hash │ Utf8?      │        │     │
  └───────────────┴────────────┴────────┴─────┘

  Storage settings:
  Store large values in "external blobs": false

  Column families:
  ┌─────────┬──────┬─────────────┬────────────────┐
  │ Name    │ Data │ Compression │ Keep in memory │
  ├─────────┼──────┼─────────────┼────────────────┤
  │ default │      │ None        │                │
  └─────────┴──────┴─────────────┴────────────────┘

  Auto partitioning settings:
  Partitioning by size: true
  Partitioning by load: false
  Preferred partition size (Mb): 2048
  Min partitions count: 1
  ```

{% endlist %}

All subsequent migration files should be created in the same way.

### Downgrading migrations

Let's downgrade (revert) the latest migration using `goose down`:
```
$ goose ydb $YDB_CONNECTION_STRING down
2024/01/12 13:07:18 OK   20240112120057_00002_add_column_password_hash_into_table_users.sql (43ms)
```

Let's check the migration statuses through `goose status`:
```
$ goose ydb $YDB_CONNECTION_STRING status
2024/01/12 13:07:36     Applied At                  Migration
2024/01/12 13:07:36     =======================================
2024/01/12 13:07:36     Fri Jan 12 11:55:18 2024 -- 20240112115229_00001_create_first_table.sql
2024/01/12 13:07:36     Pending                  -- 20240112120057_00002_add_column_password_hash_into_table_users.sql
```

Status `Fri Jan 12 12:04:56 2024` changed to `Pending` - this means that the latest migration is no longer applied.

Let's check the changes again:

{% list tabs %}

- Using YDB UI on http://localhost:8765

  ![YDB UI after apply first migration](../_assets/goose-ydb-ui-after-first-migration.png =450x)

- Using YDB CLI

  ```
  $ ydb -e grpc://localhost:2136 -d /local scheme describe users
  <table> users

  Columns:
  ┌────────────┬────────────┬────────┬─────┐
  │ Name       │ Type       │ Family │ Key │
  ├────────────┼────────────┼────────┼─────┤
  │ id         │ Uint64?    │        │ K0  │
  │ username   │ Utf8?      │        │     │
  │ created_at │ Timestamp? │        │     │
  └────────────┴────────────┴────────┴─────┘

  Storage settings:
  Store large values in "external blobs": false

  Column families:
  ┌─────────┬──────┬─────────────┬────────────────┐
  │ Name    │ Data │ Compression │ Keep in memory │
  ├─────────┼──────┼─────────────┼────────────────┤
  │ default │      │ None        │                │
  └─────────┴──────┴─────────────┴────────────────┘

  Auto partitioning settings:
  Partitioning by size: true
  Partitioning by load: false
  Preferred partition size (Mb): 2048
  Min partitions count: 1
  ```

{% endlist %}


## Short list of "goose" commands

The `goose` utility allows you to manage migrations via the command line:
- `goose status` - view the status of applying migrations. For example, `goose ydb $YDB_CONNECTION_STRING status`.
- `goose up` - apply all known migrations. For example, `goose ydb $YDB_CONNECTION_STRING up`.
- `goose up-by-one` - apply exactly one “next” migration. For example, `goose ydb $YDB_CONNECTION_STRING up-by-one`.
- `goose redo` - re-apply the latest migration. For example, `goose ydb $YDB_CONNECTION_STRING redo`.
- `goose down` - rollback the last migration. For example, `goose ydb $YDB_CONNECTION_STRING down`.
- `goose reset` - rollback all migrations. For example, `goose ydb $YDB_CONNECTION_STRING reset`.

{% note warning %}

Be careful: the `goose reset` command will revert all your migrations using your statements in blocks `+goose Down`. In many cases it might lead to all data in the database being erased. Make sure you regularly do backups and check that they can be restored to minimize impact of this risk.

{% endnote %}
