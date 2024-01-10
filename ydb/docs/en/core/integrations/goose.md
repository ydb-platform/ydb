# Versioning data schema and migration in YDB using "goose"

## Introduction

[Goose](https://github.com/pressly/goose) is an open-source tool that helps to version the data schema in the database and manage migrations between these versions. Goose supports many different database management systems, including YDB. Goose uses migration files and stores the state of migrations directly in the database in a special table.

## Install goose

Goose installation options are described in [its documentation](https://github.com/pressly/goose/blob/master/README.md#install).

## Launch arguments goose

After installation, the `goose` command line utility can be called:

```
$ goose <DB> <DSN> <COMMAND> <COMMAND_ARGUMENTS>
```

Where:
- `<DB>` - database engine, for YDB you should write `goose ydb`
- `<DSN>` - database connection string.
- `<COMMAND>` - the command to be executed. A complete list of commands is available in the built-in help (`goose help`).
- `<COMMAND_ARGUMENTS>` - command arguments.

## YDB connection string

To connect to YDB you should use a connection string like

```
<protocol>://<host>:<port>/<database_path>?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric
```

Where:
- `<protocol>` - connection protocol (`grpc` for an unsecured connection or `grpcs` for a secure (`TLS`) connection). The secure connection (with `TLS`) requires certificates. You should declare certificates like this: `export YDB_SSL_ROOT_CERTIFICATES_FILE=/path/to/ydb/certs/CA.pem`.
- `<host>` - connection address of YDB.
- `<port>` - port for connecting to YDB.
- `<database_path>` - database in the YDB cluster.
- `go_query_mode=scripting` - special `scripting` mode for executing queries by default in the YDB driver. In this mode, all requests from goose are sent to the YDB `scripting` service, which allows processing of both `DDL` and `DML` `SQL` statements.
- `go_fake_tx=scripting` - support for transaction emulation in query execution mode through the YDB `scripting` service. The fact is that in YDB, executing `DDL` `SQL` statements in a transaction is impossible (or incurs significant overhead). In particular, the `scripting` service does not allow interactive transactions (with explicit `Begin`+`Commit`/`Rollback`). Accordingly, the transaction emulation mode does not actually do anything (`nop`) on the `Begin`+`Commit`/`Rollback` calls from `goose`. This trick can, in rare cases, cause an individual migration step to end up in an intermediate state. The YDB team is working on a new `query` service that should help remove this risk.
- `go_query_bind=declare,numeric` - support for bindings of auto-inference of YQL types from query parameters (`declare`) and support for bindings of numbered parameters (`numeric`). The fact is that `YQL` is a strongly typed language that requires you to explicitly specify the types of query parameters in the body of the `SQL` query itself using the special `DECLARE` statement. Also, `YQL` only supports named query parameters (for example, `$my_arg`), while the `goose` core generates `SQL` queries with numbered parameters (`$1`, `$2`, etc.) . The `declare` and `numeric` bindings modify the original queries from `goose` at the YDB driver level, which ultimately allowed `goose` to be built into.

If connecting to a local YDB docker container, the connection string should look like:

```
grpc://localhost:2136/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric
```

Further examples of calling `goose` commands will contain exactly this connection string.

## Directory with migration files

Let's create a migrations directory and then all `goose` commands should be executed in this directory:

```
$ mkdir migrations && cd migrations
```

## Adding a migration file

The migration file can be generated using the `goose create` command:

```
$ goose ydb "grpc://localhost:2136/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric" create 00001_create_first_table sql
2023/12/15 05:22:48 Created new file: 20231215052248_00001_create_table_users.sql
```

This means that the tool has created a new migration file `<timestamp>_00001_create_table_users.sql` where we can record the steps to change the schema for the YDB database, which is accessible through the corresponding connection string.

So, after executing the goose create command, a migration file `<timestamp>_00001_create_table_users.sql` will be created with the following content:

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

The migration file consists of two sections. The first is `+goose Up`, an area where we can record the migration steps. The second is `+goose Down`, an area in which we can write a step for invert making changes for the `+goose Up` steps. `Goose` carefully inserted placeholder queries:

```
SELECT 'up SQL query';
```

And

```
SELECT 'down SQL query';
```

so that we can instead enter essentially the migration requests themselves.

Let's edit the migration file `<timestamp>_00001_create_table_users.sql` so that when applying the migration we create a table of the structure we need, and when rolling back the migration we delete the created table:

```
-- +goose Up
-- +goose StatementBegin
CREATE TABLE users (
     id Uint64,
     username Text,
     created_at TzDatetime,
     PRIMARY KEY (id)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE users;
-- +goose StatementEnd
```

All subsequent migration files should be created in the same way.

## Managing migrations

The `goose` utility allows you to manage migrations via the command line:
- `goose up` - apply all known migrations. For example, `goose ydb "grpc://localhost:2136/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric" up`.
- `goose reset` - rollback all migrations. For example, `goose ydb "grpc://localhost:2136/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric" reset`.
- `goose up-by-one` - apply exactly one “next” migration. For example, `goose ydb "grpc://localhost:2136/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric" up-by-one`.
- `goose redo` - re-apply the latest migration. For example, `goose ydb "grpc://localhost:2136/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric" redo`.
- `goose down` - rollback the last migration. For example, `goose ydb "grpc://localhost:2136/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric" down`.
- `goose status` - view the status of applying migrations. For example, `goose ydb "grpc://localhost:2136/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric" status`.
