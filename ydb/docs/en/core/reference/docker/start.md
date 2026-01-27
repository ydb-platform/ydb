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
docker_args=(
    -d                              # run container in background and print container ID
    --rm                            # automatically remove the container
    --name ydb-local                # assign a name to the container
    --hostname localhost            # hostname
    --platform linux/amd64          # platform
    -p 2135:2135                    # publish a container grpcs port to the host
    -p 2136:2136                    # publish a container grpc port to the host
    -p 8765:8765                    # publish a container http port to the host
    -p 5432:5432                    # publish a container port to the host that provides PostgreSQL compatibility
    -p 9092:9092                    # publish a container port to the host that provides Kafka compatibility
    -v $(pwd)/ydb_certs:/ydb_certs  # mount directory with TLS certificates
    -v $(pwd)/ydb_data:/ydb_data    # mount working directory
    -e GRPC_TLS_PORT=2135           # grpcs port, needs to match what's published above
    -e GRPC_PORT=2136               # grpc port, needs to match what's published above
    -e MON_PORT=8765                # http port, needs to match what's published above
    -e YDB_KAFKA_PROXY_PORT=9092    # port, needs to match what's published above
    {{ ydb_local_docker_image}}:{{ ydb_local_docker_image_tag }}
)

docker run "${docker_args[@]}"
```

{% include [index.md](_includes/rosetta.md) %}

For more information about environment variables available when running a Docker container with {{ ydb-short-name }}, see [{#T}](configuration.md).

With the parameters specified in the example above and running Docker locally, [Embedded UI](../embedded-ui/index.md) {{ ydb-short-name }} will be available at [http://localhost:8765](http://localhost:8765).

For more information about stopping and deleting a Docker container with {{ ydb-short-name }}, see [{#T}](cleanup.md).

### Overriding the configuration file

By default, when starting a Docker container for {{ ydb-short-name }}, a built-in [configuration file](../configuration/index.md) is used, which provides standard operating parameters. To override the configuration file when starting the container, you can use the `--config-path` parameter, specifying the path to your configuration file, which has been pre-mounted in the container:

```bash
docker run "${docker_args[@]}" --config-path /path/to/your/config/file
```

For users who are not experienced with Docker, it's important to understand how to properly mount a configuration file into the container. Below is a step-by-step example:

1. Run the container without specifying a configuration file and without mounting the data directory, so that it generates a default configuration:

   ```bash
   docker run -d \
     --rm \
     --name ydb-local \
     --hostname localhost \
     --platform linux/amd64 \
     -v $(pwd)/ydb_certs:/ydb_certs \
     -e GRPC_TLS_PORT=2135 \
     -e GRPC_PORT=2136 \
     -e MON_PORT=8765 \
     {{ ydb_local_docker_image}}:{{ ydb_local_docker_image_tag }}
   ```

2. Create a directory for your configuration files and copy the generated configuration file from the container directly to it:

   ```bash
   mkdir ydb_config
   docker cp ydb-local:/ydb_data/cluster/kikimr_configs/config.yaml ydb_config/my-ydb-config.yaml
   ```

3. Stop the container if it's still running, and remove the created data directory:

   ```bash
   docker stop ydb-local
   rm -rf ydb_data
   ```

4. Edit the copied configuration file `ydb_config/my-ydb-config.yaml` as needed.

5. When running the container, use the `-v` flag to mount the directory with your configuration file into the container:

   ```bash
   docker_args=(
       -d
       --rm
       --name ydb-local
       --hostname localhost
       --platform linux/amd64
       -p 2135:2135
       -p 2136:2136
       -p 8765:8765
       -v $(pwd)/ydb_data:/ydb_data
       -v $(pwd)/ydb_config:/ydb_config
       -v $(pwd)/ydb_certs:/ydb_certs
       -e GRPC_TLS_PORT=2135
       -e GRPC_PORT=2136
       -e MON_PORT=8765
       {{ ydb_local_docker_image}}:{{ ydb_local_docker_image_tag }}
   )
   
   docker run "${docker_args[@]}" --config-path /ydb_config/my-ydb-config.yaml
   ```

In this example:

 - `$(pwd)/ydb_config` - the local directory on your computer with the configuration file
 - `/ydb_config` - directory inside the container where your local directory will be mounted
 - `/ydb_config/my-ydb-config.yaml` - path to the configuration file inside the container

This way, your local configuration file becomes accessible inside the container at the specified path.
