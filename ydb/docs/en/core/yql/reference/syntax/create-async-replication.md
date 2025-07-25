# CREATE ASYNC REPLICATION

The `CREATE ASYNC REPLICATION` statement creates an [asynchronous replication instance](../../../concepts/async-replication.md).

## Syntax {#syntax}

```yql
CREATE ASYNC REPLICATION <name>
FOR <remote_path> AS <local_path> [, <another_remote_path> AS <another_local_path>]
WITH (option = value [, ...])
```

### Parameters {#params}

* `name` — a name of the asynchronous replication instance.
* `remote_path` — a relative or absolute path to a table or directory in the source database.
* `local_path` — a relative or absolute path to a target table or directory in the local database.
* `WITH (option = value [, ...])` — asynchronous replication parameters:


    * `CONNECTION_STRING` — a [connection string](../../../concepts/connect.md#connection_string) for the source database (mandatory).
    * `CA_CERT` — a [root certificate for TLS](../../../concepts/connect.md#tls-cert). Optional parameter. Can be specified if the source database supports an encrypted data interchange protocol (`CONNECTION_STRING` starts with `grpcs://`).
    * Authentication details for the source database (mandatory) depending on the authentication method:

        * [Access token](../../../recipes/ydb-sdk/auth-access-token.md):

            * `TOKEN_SECRET_NAME` — the name of the [secret](../../../concepts/datamodel/secrets.md) that contains the token.

        * [Login and password](../../../recipes/ydb-sdk/auth-static.md):

            * `USER` — a database user name.
            * `PASSWORD_SECRET_NAME` — the name of the [secret](../../../concepts/datamodel/secrets.md) that contains the password for the source database user.

* `CONSISTENCY_LEVEL` — [consistency level of replicated data](../../../concepts/async-replication.md#consistency-levels):
  * `ROW` — [row-level data consistency](../../../concepts/async-replication.md#consistency-level-row). Default mode.
  * `GLOBAL` — [global data consistency](../../../concepts/async-replication.md#consistency-level-global). Additionally can be specified:
    * `COMMIT_INTERVAL` — [change commit interval](../../../concepts/async-replication.md#commit-interval) in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) format. The default value is 10 seconds.

## Examples {#examples}

{% note tip %}

Before creating an asynchronous replication instance, you must [create](create-object-type-secret.md) a secret with authentication credentials for the source database or ensure that you have access to an existing secret.

{% endnote %}

The following statement creates an asynchronous replication instance to synchronize the `original_table` source table in the `/Root/another_database` database to the `replica_table` target table in the local database:

```yql
CREATE ASYNC REPLICATION my_replication_for_single_table
FOR original_table AS replica_table
WITH (
    CONNECTION_STRING = 'grpcs://example.com:2135/?database=/Root/another_database',
    TOKEN_SECRET_NAME = 'my_secret'
);
```

The statement above uses the token from the `my_secret` secret for authentication and the `grpcs://example.com:2135` [endpoint](../../../concepts/connect.md#endpoint) to connect to the `/Root/another_database` database.

The following statement creates an asynchronous replication instance to replicate the source tables `original_table_1` and `original_table_2` to the target tables `replica_table_1` and `replica_table_2`:

```yql
CREATE ASYNC REPLICATION my_replication_for_multiple_tables
FOR original_table_1 AS replica_table_1, original_table_2 AS replica_table_2
WITH (
    CONNECTION_STRING = 'grpcs://example.com:2135/?database=/Root/another_database',
    TOKEN_SECRET_NAME = 'my_secret'
);
```

The following statement creates an asynchronous replication instance for the objects in the `original_dir` directory:

```yql
CREATE ASYNC REPLICATION my_replication_for_dir
FOR original_dir AS replica_dir
WITH (
    CONNECTION_STRING = 'grpcs://example.com:2135/?database=/Root/another_database',
    TOKEN_SECRET_NAME = 'my_secret'
);
```

The following statement creates an asynchronous replication instance for the objects in the `/Root/another_database` database:

```yql
CREATE ASYNC REPLICATION my_replication_for_database
FOR `/Root/another_database` AS `/Root/my_database`
WITH (
    CONNECTION_STRING = 'grpcs://example.com:2135/?database=/Root/another_database',
    TOKEN_SECRET_NAME = 'my_secret'
);
```

The following statement creates an asynchronous replication instance with a TLS root certificate specified:

```yql
CREATE ASYNC REPLICATION my_consistent_replication
FOR original_table AS replica_table
WITH (
    CONNECTION_STRING = 'grpcs://example.com:2135/?database=/Root/another_database',
    TOKEN_SECRET_NAME = 'my_secret',
    CA_CERT = '-----BEGIN CERTIFICATE-----...'
);
```

The following statement creates an asynchronous replication instance in global data consistency mode (default change commit interval is 10 seconds):

```yql
CREATE ASYNC REPLICATION my_consistent_replication
FOR original_table AS replica_table
WITH (
    CONNECTION_STRING = 'grpcs://example.com:2135/?database=/Root/another_database',
    TOKEN_SECRET_NAME = 'my_secret',
    CONSISTENCY_LEVEL = 'GLOBAL'
);
```

The following statement creates an asynchronous replication instance in global data consistency mode with a one-minute change commit interval:

```yql
CREATE ASYNC REPLICATION my_consistent_replication_1min_commit_interval
FOR original_table AS replica_table
WITH (
    CONNECTION_STRING = 'grpcs://example.com:2135/?database=/Root/another_database',
    TOKEN_SECRET_NAME = 'my_secret',
    CONSISTENCY_LEVEL = 'GLOBAL',
    COMMIT_INTERVAL = Interval('PT1M')
);
```

## See also

* [ALTER ASYNC REPLICATION](alter-async-replication.md)
* [DROP ASYNC REPLICATION](drop-async-replication.md)
