# Connecting with PostgreSQL syntax

{% include [./_includes/alert.md](./_includes/alert_preview.md) %}

## Running {{ ydb-short-name }} with PostgreSQL syntax support enabled

PostgreSQL syntax compatibility is available in the Docker image: `ghcr.io/ydb-platform/local-ydb:nightly`.

Commands for starting a local Docker container with {{ ydb-short-name }} and open ports for gRPC and Embedded UI:

{% note tip %}

In this example, the containers are intentionally created in a way that their state is deleted after they stop. This simplifies the instructions and allows you to repeatedly run tests in a known environment without worrying about past test failures.

To preserve the container's state, you need to remove the environment variable `YDB_USE_IN_MEMORY_PDISKS`.

{% endnote %}

{% list tabs %}

- Docker-compose

    To launch using a Docker Compose configuration file, it must already be [installed on your system](https://docs.docker.com/compose/install/standalone/).

    docker-compose.yaml:

    ```yaml
    services:
        ydb:
            image: ghcr.io/ydb-platform/local-ydb:nightly
            ports:
            - "2136:2136"
            - "8765:8765"
            environment:
            - "YDB_USE_IN_MEMORY_PDISKS=true"
            - "YDB_FEATURE_FLAGS=enable_temp_tables,enable_table_pg_types,enable_pg_syntax"
    ```

    Run:

    ```bash
    docker-compose up -d --pull=always
    ```

- Docker command

    ```bash
    docker run --name ydb-local -d --pull always -p 2136:2136 -p 8765:8765 \
        -e YDB_FEATURE_FLAGS=enable_temp_tables,enable_table_pg_types,enable_pg_syntax \
        -e YDB_USE_IN_MEMORY_PDISKS=true ghcr.io/ydb-platform/local-ydb:nightly
    ```

{% endlist %}

After launching the container, connect with {{ ydb-short-name }} CLI on `grpc://localhost:2136` and database `/local`, or open the [web interface](http://localhost:8765) on port 8765.

## Connecting to the running container via {{ ydb-short-name }} CLI

Use the PostgreSQL dialect marker `--!syntax_pg` at the beginning of each query:

```bash
ydb -e grpc://localhost:2136 -d /local sql -s '--!syntax_pg
SELECT ''Hello, world!'';
'
```

Output:

```text
 column0
---------------
 Hello, world!
```

### Creating a table

```bash
ydb -e grpc://localhost:2136 -d /local sql -s '--!syntax_pg
CREATE TABLE example
(
    key int4,
    value text,
    PRIMARY KEY (key)
);
'
```

### Adding test data

```bash
ydb -e grpc://localhost:2136 -d /local sql -s '--!syntax_pg
INSERT INTO example (key, value)
VALUES (123, ''hello''),
       (321, ''world'');
'
```

### Querying test data

```bash
ydb -e grpc://localhost:2136 -d /local sql -s '--!syntax_pg
SELECT COUNT(*) FROM example;
'
```

## Stopping the container

{% list tabs %}

- Docker-compose

    In the directory containing the `docker-compose.yaml` file, execute the command that will stop the container and remove its data:

    ```bash
    docker-compose down -vt 1
    ```

- Docker command

    ```bash
    docker rm -f ydb-local
    ```

{% endlist %}
