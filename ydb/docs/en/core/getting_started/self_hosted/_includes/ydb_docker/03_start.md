## Starting {#start}

The YDB Docker container uses the host system resources (CPU, RAM) within the limits allocated by the Docker settings.

The YDB Docker container stores data in its file system whose sections are reflected in the host system directory. The start container command given below will create files in the current directory, so first create a working directory and then start the container from it:

{% list tabs %}

- Disk storage

    ```bash
    docker run -d --rm --name ydb-local -h localhost \
      -p 2135:2135 -p 8765:8765 -p 2136:2136 \
      -v $(pwd)/ydb_certs:/ydb_certs -v $(pwd)/ydb_data:/ydb_data \
      -e YDB_DEFAULT_LOG_LEVEL=NOTICE \
      -e GRPC_TLS_PORT=2135 -e GRPC_PORT=2136 -e MON_PORT=8765 \
      {{ ydb_local_docker_image}}:{{ ydb_local_docker_image_tag }}
    ```

    {% note warning %}

    Currently, disk storage is not supported on Apple Silicon (M1 or M2). Use the command from "In-memory storage" tab if you want to try {{ ydb-short-name }} on this CPU.

    {% endnote %}

- In-memory storage

    ```bash
    docker run -d --rm --name ydb-local -h localhost \
      -p 2135:2135 -p 8765:8765 -p 2136:2136 \
      -v $(pwd)/ydb_certs:/ydb_certs -v $(pwd)/ydb_data:/ydb_data \
      -e YDB_DEFAULT_LOG_LEVEL=NOTICE \
      -e GRPC_TLS_PORT=2135 -e GRPC_PORT=2136 -e MON_PORT=8765 \
      -e YDB_USE_IN_MEMORY_PDISKS=true \
      {{ ydb_local_docker_image}}:{{ ydb_local_docker_image_tag }}
    ```

{% endlist %}

If started successfully, you'll see the ID of the created container.

### Startup parameters {#start-pars}

`-d` means running the Docker container in the background.

`--rm` means removing the container after its operation is completed.

`--name` is the container name. Specify `ydb-local` to be able to perform the below instructions for stopping the container by copying text through the clipboard.

`-h` is the container host name. Be sure to pass the `localhost` value. Otherwise, the container will be started with a random hostname.

`-v`: Mount the host system directories into a container as `<host system directory>:<mount directory within the container>`. The YDB container uses the following mount directories:
- `/ydb_data` for storing data. If this directory is not mounted, the container will be started without saving data to the host system disk.
- `/ydb_certs` for storing certificates to establish a TLS connection. The started container will write to this directory the certificates to be used for a TLS client connection. If this directory is not mounted, you won't be able to connect via TLS due to having no certificate information.

`-e` means setting environment variables in `<name>=<value>` format. The YDB container uses the following environment variables:
- `YDB_DEFAULT_LOG_LEVEL`: The logging level. Acceptable values: `CRIT`, `ERROR`, `WARN`, `NOTICE`, and `INFO`. Defaults to `NOTICE`.
- `GRPC_PORT`: The port for unencrypted connections. Defaults to 2136.
- `GRPC_TLS_PORT`: The port for TLS connections. Defaults to 2135.
- `MON_PORT`: The port for the built-in web UI with [monitoring and introspection](../../../../maintenance/embedded_monitoring/ydb_monitoring.md) tools. Defaults to 8765.
- `YDB_PDISK_SIZE`: The size of the data storage disk in `<NUM>GB` format (for example, `YDB_PDISK_SIZE=128GB`). Acceptable values: `80GB` and higher. Defaults to 80GB.
- `YDB_USE_IN_MEMORY_PDISKS`: Using disks in memory. Acceptable values are `true` and `false`, defaults to `false`. If enabled, the container's file system is not used for working with data, all data is only stored in the memory of a process and is lost when it's stopped. Currently, you can start the container on Apple Silicon (M1 or M2) in this mode only.

{% include [_includes/storage-device-requirements.md](../../../../_includes/storage-device-requirements.md) %}

`-p` means publishing container ports on the host system. All applicable ports must be explicitly listed even if default values are used.

{% note info %}

It may take several minutes to initialize the Docker container, depending on the allocated resources. The database will not be available until the container is initialized.

{% endnote %}
