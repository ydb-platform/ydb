# {{ ydb-short-name }} compatibility with PostgreSQL

{% include [./_includes/alert.md](./_includes/alert_preview.md) %}

PostgreSQL compatibility was an experimental mechanism for executing SQL queries in the PostgreSQL dialect on {{ ydb-short-name }} infrastructure. It allowed developers to submit PostgreSQL-syntax queries through {{ ydb-short-name }} tools (using the `--!syntax_pg` marker) or through the PostgreSQL wire protocol (pgwire).

**This functionality has been removed.** {{ ydb-short-name }} no longer accepts the `--!syntax_pg` marker, does not expose the PostgreSQL wire protocol, and no longer provides the `ydb tools pg-convert` utility.

To migrate applications from PostgreSQL to {{ ydb-short-name }}, rewrite queries in [YQL](../yql/reference/index.md) or use the [SQL dialect converter](../integrations/sql-dialect-converter.md). To import data, see [data migration](../integrations/data-migration/index.md) and [import from files](../reference/ydb-cli/export-import/import-file.md).

The remaining pages in this section describe PostgreSQL constructs that were supported previously and are kept for reference.
