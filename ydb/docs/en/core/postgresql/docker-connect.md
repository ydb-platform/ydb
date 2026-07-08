# Connecting with PostgreSQL syntax

{% include [./_includes/alert.md](./_includes/alert_preview.md) %}

PostgreSQL SQL dialect support and the `enable_pg_syntax` feature flag were **removed** from {{ ydb-short-name }}. It is no longer possible to run a local Docker container with PostgreSQL syntax enabled or to prepend queries with `--!syntax_pg`.

To run a local {{ ydb-short-name }} instance for development and testing, follow the [Docker quick start](../reference/docker/start.md). Connect with {{ ydb-short-name }} CLI on `grpc://localhost:2136` and database `/local`, or open the [web interface](http://localhost:8765) on port 8765.

Example:

```bash
ydb -e grpc://localhost:2136 -d /local sql -s 'SELECT 1;'
```
