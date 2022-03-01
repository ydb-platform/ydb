# Starting a local cluster {{ ydb-short-name }}

This section describes how to deploy a local {{ ydb-short-name }} cluster using a configuration in YAML format.

{% note warning %}

When a local database is running, some tasks may require a significant portion of the host system resources.

{% endnote %}

## Download {{ ydb-short-name }} {#download-ydb}

Download and unpack an archive with the `ydbd` executable file and the libraries necessary for working with {{ ydb-short-name }}. Next, go to the directory with the artifacts:

```bash
curl https://binaries.ydb.tech/ydbd-master-linux-amd64.tar.gz | tar -xz
cd ydbd-master-linux-amd64/
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:`pwd`/lib"
```

## Prepare a local cluster configuration {#prepare-configuration}

Prepare a local cluster configuration that you want to deploy. To run a cluster with in-memory data storage, just copy the configuration. To deploy a cluster with data storage in a file, additionally create a 64GB file and specify the path to it in the configuration.

{% list tabs %}

- In memory

  ```bash
  wget https://raw.githubusercontent.com/ydb-platform/ydb/main/ydb/deploy/yaml_config_examples/single-node-in-memory.yaml -O config.yaml
  ```

- In a file

  ```bash
  wget https://raw.githubusercontent.com/ydb-platform/ydb/main/ydb/deploy/yaml_config_examples/single-node-with-file.yaml -O config.yaml
  dd if=/dev/zero of=/tmp/ydb-local-pdisk.data bs=1 count=0 seek=68719476736
  sed -i "s/\/tmp\/pdisk\.data/\/tmp\/ydb-local-pdisk\.data/g" config.yaml
  ```

{% endlist %}

## Start a static cluster node {#start-static-node}

Start a static cluster node by running the command:

```bash
./bin/ydbd server --yaml-config ./config.yaml --node 1 --grpc-port 2135 --ic-port 19001 --mon-port 8765
```

Initialize storage with the command:

```bash
./bin/ydbd admin blobstorage config init --yaml-file ./config.yaml
```

## Creating the first database {#create-db}

To work with tables, you need to create at least one database and run a process serving this database. To do this, you need to run a set of commands:

Create a database:

```bash
./bin/ydbd admin database /<domain-name>/<db-name> create <storage-pool-kind>:<storage-unit-count>
```

For example,

```bash
./bin/ydbd admin database /Root/test create ssd:1
```

Start the process serving the database:

```bash
./bin/ydbd server --yaml-config ./config.yaml --tenant /<domain-name>/<db-name> --node-broker <address>:<port> --grpc-port 31001 --ic-port 31003 --mon-port 31002
```

For example, to run the process for the database created above, execute the following command.

```bash
./bin/ydbd server --yaml-config ./config.yaml --tenant /Root/test --node-broker localhost:2135 --grpc-port 31001 --ic-port 31003 --mon-port 31002
```

## Working with the database via the Web UI {#web-ui}

To view the database structure and execute a YQL query, use the web interface embedded in the `ydbd` process. To do this, open your browser and go to `http://localhost:8765`. For more details about the embedded web interface, see [Embedded UI](../maintenance/embedded_monitoring/ydb_monitoring.md).

