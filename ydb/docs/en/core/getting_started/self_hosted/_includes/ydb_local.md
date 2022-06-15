# Running {{ ydb-short-name }} from a binary file

This section describes how to deploy a local single-node {{ ydb-short-name }} cluster using a compiled binary file. Currently, the system **only supports builds for Linux**. Support for Windows and MacOS builds will be added later.

## Connection parameters {#conn}

As a result of completing the steps below, you'll get a YDB database running on a local machine that you can connect to using the following:

- [Endpoint](../../../concepts/connect.md#endpoint): `grpc://localhost:2136`
- [Database location](../../../concepts/connect.md#database): `/Root/test`
- [Authentication](../../../concepts/connect.md#auth-modes): Anonymous (without authentication)

## Installation {#install}

Create a working directory. Run the script for downloading an archive with the `ydbd` executable file and the necessary {{ ydb-short-name }} libraries, and a set of scripts and auxiliary files for starting and stopping the server:

```bash
curl https://binaries.ydb.tech/local_scripts/install.sh | bash
```

{% include [wget_auth_overlay.md](wget_auth_overlay.md) %}

## Starting {#start}

The local YDB server can be started in disk or in-memory mode:

{% list tabs %}

- Storing data on disk

  - To store data on disk, when running the script for the first time, a 64 GB file named `ydb.data` is created in the working directory. Make sure you have enough free space to create it.

  - Run the following command from the working directory:

    ```bash
    ./start.sh disk
    ```

- Storing data in memory

  - When using in-memory data storage, the data is lost when stopping the server.

  - Run the following command from the working directory:

    ```bash
    ./start.sh ram
    ```

{% endlist %}

The YDB server is started in the context of the current terminal window. Closing the terminal window stops the server.

If you get an error saying `Failed to set up IC listener on port 19001 errno# 98 (Address already in use)` at startup, the server might have already been started and you should stop it with the `stop.sh` script (see below).

## Stopping {#stop}

To stop the server, run the following command in the working directory:

```bash
./stop.sh
```

## Making queries via the YDB CLI {#cli}

[Install the YDB CLI](../../../reference/ydb-cli/install.md) and execute queries as described in [YDB CLI - Getting started](../../cli.md) using the endpoint and database location specified [at the beginning of this article](#conn). For example:

```bash
ydb -e grpc://localhost:2136 -d /Root/test scheme ls
```

## Working with the database via the Web UI {#web-ui}

To work with the DB structure and data, you can also use the web interface embedded in the `ydbd` process. It is available at `http://localhost:8765`. For more information about the embedded web interface, see [Embedded UI](../../../maintenance/embedded_monitoring/ydb_monitoring.md).


## Monitoring database using Grafana and Prometheus {#dashboards}

The local YDB server may be integrated with [Prometheus](https://prometheus.io/) and [Grafana](https://grafana.com/), popular open source tools for metrics gathering and visualization. To setup standard [dashboards](../../../troubleshooting/grafana_dashboards.html), follow these instructions:

1. [Install and run](https://prometheus.io/docs/prometheus/latest/getting_started/#downloading-and-running-prometheus) Prometheus with this [configuration file](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/grafana_dashboards/local_ydb_prometheus.yml).
1. [Install and run](https://grafana.com/docs/grafana/latest/getting-started/getting-started/) Grafana.
1. [Create](https://prometheus.io/docs/visualization/grafana/#creating-a-prometheus-data-source) datasource of type "prometheus" in Grafana and connect it to your Prometheus instance.
1. Upload [dashboards](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/grafana_dashboards/) to Grafana. You may upload dashboards manually via Grafana UI [Import](https://grafana.com/docs/grafana/latest/dashboards/export-import/#import-dashboard) function or use simple [script](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/grafana_dashboards/local_upload_dashboards.sh). Please note that the script uses [basic authentification](https://grafana.com/docs/grafana/latest/http_api/create-api-tokens-for-org/#authentication) in Grafana. For other cases, adjust the script accordingly.


## Advanced options {#advanced}

Instructions on how to deploy multi-node clusters and configure them are given in [Deploy YDB on-premises](../../../deploy/manual/deploy-ydb-on-premises.md) in the "Cluster management" section.

