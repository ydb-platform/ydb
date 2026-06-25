
{% note warning %}

At the moment, {{ ydb-short-name }}'s compatibility with PostgreSQL **is under development**, so not all PostgreSQL constructs and [functions](../functions.md) are supported yet. The PostgreSQL wire protocol listener is **disabled by default** on `ydbd` nodes and should not be enabled on production clusters. For local testing, use the [local Docker image](../reference/docker/start.md), which enables pgwire by default, or follow the [dedicated instructions](docker-connect.md).

{% endnote %}
