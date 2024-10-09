# Running {{ ydb-short-name }} in Docker

## Before start

Create a folder for testing {{ ydb-short-name }} and use it as the current working directory:

```bash
mkdir ~/ydbd && cd ~/ydbd
mkdir ydb_data
mkdir ydb_certs
```

## Launching a container with {{ ydb-short-name }} in Docker

Example of the {{ ydb-short-name }} startup command in Docker with detailed comments:

```bash
docker run \
    -d \ # run container in background and print container ID
    --rm \ # automatically remove the container
    --name ydb-local \ # assign a name to the container
    -h localhost \ # hostname
    -p 2135:2135 \ # publish a container grpcs port to the host 
    -p 2136:2136 \ # publish a container grpc port to the host 
    -p 8765:8765 \ # publish a container http port to the host 
    -p 5432:5432 \ # publish a container port to the host, providing PostgreSQL compatibility
    -v $(pwd)/ydb_certs:/ydb_certs \ # directory with TLS certificates
    -v $(pwd)/ydb_data:/ydb_data \ # working directory
    -e GRPC_TLS_PORT=2135 \ # grpcs port
    -e GRPC_PORT=2136 \ # grpc port
    -e MON_PORT=8765 \ # http port
    -e YDB_USE_IN_MEMORY_PDISKS=1 \ # makes all data volatile and stored only in RAM
    ydbplatform/local-ydb:latest # docker image name and tag
```

For more information about environment variables used when running a Docker container with {{ ydb-short-name }}, see [{#T}](environment.md)

With the parameters specified in the example above and running Docker locally, [Embedded UI](../embedded-ui/index.md) {{ ydb-short-name }} will be available at [http://localhost:8765](http://localhost:8765).

For more information about stopping and deleting a Docker container with {{ ydb-short-name }}, see [{#T}](cleanup.md)
