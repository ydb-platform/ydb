# PostgreSQL compatibility (removed)

{% note alert %}

**This feature has been removed.** Experimental PostgreSQL compatibility — the PostgreSQL wire protocol (pgwire), PostgreSQL SQL dialect (`--!syntax_pg`), and related tooling — is no longer available in {{ ydb-short-name }}.

{% endnote %}

{{ ydb-short-name }} previously provided an **experimental** PostgreSQL compatibility layer. It was never intended for production use and has been removed from the database server.

## What was removed

The following capabilities are **no longer supported**:

| Capability | Description |
| --- | --- |
| **PostgreSQL wire protocol (pgwire)** | Connecting with psql, pgAdmin, and PostgreSQL drivers (lib/pq, psycopg2, JDBC in PostgreSQL mode) on port 5432 |
| **PostgreSQL SQL dialect** | Queries with `--!syntax_pg`, `ydb sql --syntax pg`, and the `enable_pg_syntax` cluster setting |
| **PostgreSQL-oriented tooling** | `ydb tools pg-convert` and workflows based on `pg_dump` import into the compatibility layer |
| **PostgreSQL-style system views** | `.sys/pg_tables`, `.sys/pg_class`, and related pgwire catalog views |

## What to use instead

| If you used | Switch to |
| --- | --- |
| psql, pgAdmin, or PostgreSQL drivers | [{{ ydb-short-name }} CLI](../reference/ydb-cli/index.md), [Embedded UI](../reference/embedded-ui/index.md), [{{ ydb-short-name }} SDK](../reference/ydb-sdk/index.md), or the [JDBC driver](https://github.com/ydb-platform/ydb-jdbc-driver) |
| PostgreSQL SQL syntax | [YQL](../yql/reference/index.md) — {{ ydb-short-name }}'s native SQL dialect |
| `pg_dump` + `pg-convert` import | [YDB tools dump/restore](../reference/ydb-cli/export-import/tools-dump.md) or [data migration integrations](../integrations/data-migration/index.md) |
| BI tools via pgwire (for example, FineBI) | Native connectors: [Superset](../integrations/visualization/superset.md), [Grafana](../integrations/visualization/grafana.md), or a driver supported by your tool |

For a step-by-step overview of supported client options, see [Getting started](../dev/getting-started.md).

## What is still available

Removing PostgreSQL compatibility does **not** affect these separate features:

* **[Federated Query](../concepts/query_execution/federated_query/postgresql.md)** — run YQL queries against **external** PostgreSQL clusters
* **[Pg:: UDFs](../yql/reference/udf/list/postgres.md)** — PostgreSQL-style functions and literals inside YQL
* **PostgreSQL column types** (`pgint4`, `pgtext`, and others) — optional table column types in YQL, controlled by the `enable_table_pg_types` setting

## Questions and feedback

If you need help migrating from PostgreSQL compatibility, discuss your case in [{{ ydb-short-name }} Discord](https://discord.gg/R5MvZTESWc) or open an issue on [GitHub](https://github.com/ydb-platform/ydb/issues).
