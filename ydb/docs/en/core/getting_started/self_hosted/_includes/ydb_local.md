# Running {{ ydb-short-name }} from a binary file

This section describes how to deploy a local single-node {{ ydb-short-name }} cluster using a built binary file. Currently, **only a Linux build** is supported. Support for Windows and MacOS builds will be added later.

## Connection parameters {#conn}

As a result of completing the steps described below, you'll get a YDB database running on your local machine, which you can connect to using the following parameters:

- [Endpoint](../../../concepts/connect.md#endpoint): `grpc://localhost:2136`
- [DB path](../../../concepts/connect.md#database): `/Root/test`
- [Authentication](../../../concepts/auth.md): Anonymous (no authentication)

## Installing {#install}

Create a working directory. In this directory, run a script to download an archive with the `ydbd` executable file and libraries required for using {{ ydb-short-name }}, as well as a set of scripts and auxiliary files to start and stop a server:

```bash
curl {{ ydbd-install-url }} | bash
```

{% include [wget_auth_overlay.md](wget_auth_overlay.md) %}

## Starting {#start}

You can start a local YDB server with a disk or in-memory storage:

{% list tabs %}

- Disk storage

   - {% include [_includes/storage-device-requirements.md](../../../_includes/storage-device-requirements.md) %}

   - The first time you run the script, an 80GB `ydb.data` file will be created in the working directory. Make sure there's enough disk space to create it.

   - Run the following command from the working directory:

      ```bash
      ./start.sh disk
      ```

- In-memory storage

   - When data is stored in memory, it will be lost if the server is stopped.

   - Run the following command from the working directory:

      ```bash
      ./start.sh ram
      ```

{% endlist %}

The YDB server is started in the current terminal window context. Closing the window stops the server.

If you see an error saying `Failed to set up IC listener on port 19001 errno# 98 (Address already in use)` when you make an attempt to start the server, then the server may already be running and you should stop it using the `stop.sh` script (see bellow).

## Stopping {#stop}

To stop the server, run the following command in the working directory:

```bash
./stop.sh
```

## Making queries via the YDB CLI {#cli}

[Install](../../../reference/ydb-cli/install.md) the YDB CLI and run a query, for example:

```bash
ydb -e grpc://localhost:2136 -d /Root/test scheme ls
```

## Working with the database via the Web UI {#web-ui}

To work with the DB structure and data, you can also use a web interface that is embedded in the `ydbd` process and available at `http://localhost:8765`. For more details about the embedded web interface, see [Embedded UI](../../../maintenance/embedded_monitoring/ydb_monitoring.md).

## Additional features {#advanced}

For information about how to deploy multi-node clusters and configure them, see [Managing a cluster](../../../deploy/index.md).
