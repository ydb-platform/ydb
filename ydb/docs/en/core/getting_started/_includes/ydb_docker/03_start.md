## Run a YDB Docker container {#start}

Run a {{ ydb-short-name }} Docker container and mount the necessary directories:

```bash
docker run -d \
  --rm \
  --name ydb-local \
  -h localhost \
  -p 2135:2135 -p 8765:8765 \
  -v $(pwd)/ydb_certs:/ydb_certs -v $(pwd)/ydb_data:/ydb_data \
  -e YDB_LOCAL_SURVIVE_RESTART=true \
  cr.yandex/yc/yandex-docker-local-ydb:latest
```

Where:

* `-d` means running the Docker container in the background.
* `--rm` means removing the container after its operation is completed.
* `--name` is the container name.
* `-h` is the container host name.
* `-p` means publishing container ports on the host system.
* `-v` means mounting the host system directories into a container.
* `-e` means setting environment variables.

{% note info %}

It may take several minutes to initialize the Docker container, depending on the capacity of the host system. The database will not be available until the container is initialized.

{% endnote %}

{{ ydb-short-name }} Docker containers support some additional options that can be set through environment variables:

* `YDB_LOCAL_SURVIVE_RESTART=true` enables you to restart a container without losing data.
* `YDB_USE_IN_MEMORY_PDISKS=true` enables you to store all your data in memory. If this option is enabled, restarting the container with a local {{ ydb-short-name }} instance will result in complete data loss.
* `YDB_DEFAULT_LOG_LEVEL=<level>` enables you to set the logging level.. Valid levels: `CRIT`, `ERROR`, `WARN`, `NOTICE`, `INFO`.
