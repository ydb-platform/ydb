# Connecting via PostgreSQL Protocol

## Running {{ ydb-short-name }} with PostgreSQL compatibility enabled

Currently, the PostgreSQL compatibility feature is available for testing in the Docker image: `ghcr.io/ydb-platform/local-ydb:nightly`.

Commands for starting a local Docker container with YDB and open ports for PostgreSQL and Web-UI:

{% note tip %}

In this example, the containers are intentionally created in a way that their state is deleted after they stop. This simplifies the instructions and allows you to repeatedly run tests in a known environment without worrying about past test failures.

To preserve the container's state, you need to remove the environment variable `YDB_USE_IN_MEMORY_PDISKS`.

{% endnote %}

{% list tabs %}

- Docker-compose

    To launch using a Docker Compose configuration file, it must already be [installed on your system](https://docs.docker.com/compose/install/standalone/).

    docker-compose.yaml:
    ```
    services:
        ydb:
            image: ghcr.io/ydb-platform/local-ydb:nightly
            ports:
            - "5432:5432"
            - "8765:8765"
            environment:
            - "YDB_USE_IN_MEMORY_PDISKS=true"
            - "POSTGRES_USER=${YDB_PG_USER:-root}"
            - "POSTGRES_PASSWORD=${YDB_PG_PASSWORD:-1234}"
            - "YDB_EXPERIMENTAL_PG=1"
    ```

    Run:
    ```bash
    docker-compose up -d --pull=always
    ```

- Docker command

    ```bash
    docker run --name ydb-postgres -d --pull always -p 5432:5432 -p 8765:8765 -e POSTGRES_USER=root -e POSTGRES_PASSWORD=1234 -e YDB_EXPERIMENTAL_PG=1 -e YDB_USE_IN_MEMORY_PDISKS=true ghcr.io/ydb-platform/local-ydb:nightly
    ```

{% endlist %}

After launching the container, you can connect to it via PostgreSQL clients on port 5432, the database `local`, or open the [web interface](http://localhost:8765) on port 8765.

## Connecting to the Running Container via psql

Upon executing this command, the interactive command-line interface of `psql`, the PostgreSQL client, will be launched. All subsequent queries should be entered within this client interface.

```bash
docker run --rm -it --network=host postgres:14 psql postgresql://root:1234@localhost:5432/local
```

### Hello world example

```sql
SELECT 'Hello, world!';
```

Output:
```
    column0
---------------
 Hello, world!
(1 row)
```

### Creating a Table
The primary purpose of database management systems is to store data for later retrieval. As an SQL-based system, the principal abstraction for storing data is the table. To create our first table, execute the following query:

```sql

CREATE TABLE example
(
    key int4,
    value text,
    PRIMARY KEY (key)
);
```

### Adding test data
Now let's populate our table with some initial data. The simplest way to do this is by using literals.


```sql
INSERT INTO example (key, value)
VALUES (123, 'hello'),
       (321, 'world');
```

### Querying test data

```sql
SELECT COUNT(*) FROM example;
```

Output:
```
 column0
---------
       2
(1 row)
```


## Stopping the Container

This command will stop the running container and delete all data stored within it.

{% list tabs %}

- Docker-compose

    In the directory containing the `docker-compose.yaml` file, execute the command that will stop the container and remove its data:

    ```bash
    docker-compose down -vt 1
    ```
    {% note info %}

    To stop a Docker container and preserve its data, you should run it without the `YDB_USE_IN_MEMORY_PDISKS` environment variable and use the stop command:

    ```bash
    docker-compose stop
    ```

    {% endnote %}

- Docker command

    This command will stop the container and remove the data:

    ```bash
    docker rm -f ydb-postgres
    ```

{% endlist %}
