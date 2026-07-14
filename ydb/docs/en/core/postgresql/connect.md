# Executing queries

{% include [./_includes/alert.md](./_includes/alert_preview.md) %}

There are several ways to run queries in the PostgreSQL dialect:

1. Using native PostgreSQL tools. {{ ydb-name }} has a compatibility service to support the native PostgreSQL network protocol - [pgwire](https://www.postgresql.org/docs/current/protocol.html), so standard PostgreSQL clients can connect to {{ ydb-name }}.
2. Using native {{ ydb-short-name }} tools and explicitly specifying that the query is executed in PostgreSQL syntax using the parameters (settings) of these tools when executing queries.
3. Using any tools that interact with {{ ydb-short-name }} and explicitly specifying that the query is executed in PostgreSQL syntax using a special comment at the beginning of the query body `--!syntax_pg`.

## Native PostgreSQL network protocol {#pgwire}

To connect to {{ ydb-name }} via the PostgreSQL protocol, you can use any client libraries and applications that support this protocol. Below is an example of connecting using the `psql` utility, which is included in the PostgreSQL distribution.

To connect, you need to perform the following preliminary steps:

1. Get the address of the cluster you are connecting to.
2. Get the name of the database in the cluster you are connecting to.

Execute a query in {{ydb-name}}:


```bash
psql -h <ydb_address> -U <user_name> <database_name>
```


Where:

- `<ydb_address>` - address of the {{ ydb-short-name }} cluster you are connecting to.
- `<database_name>` - name of the database in the cluster. It can be a complex name, for example, `mycluster/tenant1/database`.
- `<user_name>` - user login.

## Using standard {{ydb-name}} tools {#syntaxpg}

You can execute queries in the PostgreSQL dialect using any {{ydb-name}} tools by explicitly specifying the dialect indicator at the beginning of the query body using a special comment: `--!syntax_pg`.

To connect, you need to perform the following preliminary steps:

1. Get the address of the [cluster](../concepts/glossary.md#cluster) you are connecting to.
2. Get the name of the [database](../concepts/glossary.md#database) in the cluster you are connecting to.

Execute a query to {{ ydb-name }}:


```bash
ydb -e <ydb_address> -d <database_name> --user <user_name> sql -s '--!syntax_pg
SELECT 1;
'
```


Where:

- `<ydb_address>` — address of the {{ ydb-short-name }} cluster you are connecting to.
- `<database_name>` — name of the database in the cluster. It can be a complex name, for example, `mycluster/tenant1/database/`.
- `<user_name>` — user login.
