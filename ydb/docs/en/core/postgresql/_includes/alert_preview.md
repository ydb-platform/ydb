
{% note warning %}

At the moment, {{ ydb-short-name }}'s compatibility with PostgreSQL **is under development**, so not all PostgreSQL constructs and [functions](../functions.md) are supported yet. The PostgreSQL wire protocol listener is **disabled by default** on `ydbd` nodes and in the local Docker image; it should not be enabled on production clusters. For local testing, set `YDB_EXPERIMENTAL_PG=1` when starting the container — see [these instructions](docker-connect.md).

{% endnote %}
