# Using the YDB Docker container

For debugging or testing, you can run a {{ ydb-full-name }} instance in a Docker container. Docker containers use the current {{ ydb-short-name }} build version, but the build revision may differ.

In local launch mode, you can only interact with a database using the {{ ydb-short-name }} API. The API is available at the `grpcs://localhost:2135` endpoint. The database name is `/local`. To work with your database, you can use the {{ ydb-short-name }} command-line client ([YDB CLI](../../../reference/ydb-cli/index.md)) built into the Docker image.

{{ ydb-full-name }} in a Docker container accepts incoming TLS-encrypted connections. Certificates are generated automatically. To use certificates, you need to mount the `/ydb_cert`  directory on the host system of a Docker container.

To save the database state locally, mount the `/ydb_data` directory on the host system of a Docker container.

{% note warning %}

When a local database is running, some tasks may require a significant portion of the host system resources.

{% endnote %}
