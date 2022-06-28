# Deploy {{ ydb-short-name }} On-Premises

This document describes how to deploy a multi-tenant {{ ydb-short-name }} cluster on multiple servers.

## Before you start {#before-start}

### Prerequisites {#requirements}

Make sure you have SSH access to all servers. This is necessary to install the binaries, configuration files and run the {{ ydb-short-name }} executable file.
Your network configuration must allow TCP connections on the following ports (by default):

* 2135, 2136: GRPC for client-cluster interaction.
* 19001, 19002 - Interconnect for intra-cluster node interaction.
* 8765, 8766: The HTTP interface for cluster monitoring.

<!--Check out the [Production checklist](../production_checklist.md) and the recommended cluster topology;-->

Select the servers and disks to be used for data storage:

* Use the `block-4-2` fault tolerance model for cluster deployment in one availability zone (AZ). To survive the loss of 2 nodes, use at least 8 nodes.
* Use the `mirror-3-dc` fault tolerance model for cluster deployment in three availability zones (AZ). To survive the loss of 1 AZ and 1 node in another AZ, use at least 9 nodes. The number of nodes in each AZ should be the same.

Run each static node on a separate server.

## Create a system user and a group to run {{ ydb-short-name }} under {#create-user}

On each server where {{ ydb-short-name }} will be running, execute:

```bash
sudo groupadd ydb
sudo useradd ydb -g ydb
```

To make sure the {{ ydb-short-name }} server has access to block store disks to run, add the user to start the process under to the disk group.

```bash
sudo usermod -aG disk ydb
```

## Prepare and format disks on each server {#prepare-disks}

{% note warning %}

We don't recommend using disks that are used by other processes (including the OS) for data storage.

{% endnote %}

1\. Create a partition on the selected disk

{% note alert %}

Be careful! The following step will delete all partitions on the specified disks.
Make sure that you specified the disks that have no other data!

{% endnote %}

```bash
sudo parted /dev/nvme0n1 mklabel gpt -s
sudo parted -a optimal /dev/nvme0n1 mkpart primary 0% 100%
sudo parted /dev/nvme0n1 name 1 ydb_disk_ssd_01
sudo partx --u /dev/nvme0n1
```

As a result, a disk labeled as `/dev/disk/by-partlabel/ydb_disk_ssd_01` will appear in the system.

If you plan to use more than one disk on each server, specify a label that is unique for each of them instead of `ydb_disk_ssd_01`. You'll need to use these disks later in the configuration files.

2\. Download an archive with the `ydbd` executable file and the libraries necessary for working with {{ ydb-short-name }}:

```bash
curl https://binaries.ydb.tech/ydbd-main-linux-amd64.tar.gz | tar -xz
```

3\. Create directories to run {{ ydb-short-name }}:

```bash
sudo mkdir -p /opt/ydb/bin /opt/ydb/cfg /opt/ydb/lib
sudo chown -R ydb:ydb /opt/ydb
```

4\. Copy the binary file, libraries, and configuration file to the appropriate directories:

```bash
sudo cp -i ydbd-main-linux-amd64/bin/ydbd /opt/ydb/bin/
sudo cp -i ydbd-main-linux-amd64/lib/libaio.so /opt/ydb/lib/
sudo cp -i ydbd-main-linux-amd64/lib/libiconv.so /opt/ydb/lib/
sudo cp -i ydbd-main-linux-amd64/lib/libidn.so /opt/ydb/lib/
```

5\. Format the disk with the built-in command

```bash
sudo LD_LIBRARY_PATH=/opt/ydb/lib /opt/ydb/bin/ydbd admin bs disk obliterate /dev/disk/by-partlabel/ydb_disk_ssd_01
```

Perform this operation for each disk that will be used for data storage.

## Choose the network communication mode

Prepare the configuration files, depending on the network communication mode chosen:

{% list tabs %}

- Unprotected mode

  In this mode, the traffic between cluster nodes and between client and cluster uses an unencrypted connection. Use this mode for testing purposes.

  {% include [prepare-configs.md](_includes/prepare-configs.md) %}

  Save the {{ ydb-short-name }} configuration file as `/opt/ydb/cfg/config.yaml`

- Protected mode

  In this mode, the traffic between cluster nodes and between the client and cluster is encrypted using the TLS protocol.

  {% include [generate-ssl.md](_includes/generate-ssl.md) %}

  Create directories for certificates on each node

  ```bash
  sudo mkdir /opt/ydb/certs
  sudo chown -R ydb:ydb /opt/ydb/certs
  sudo chmod 0750 /opt/ydb/certs
  ```

  Copy the node certificates and keys

  ```bash
  sudo -u ydb cp certs/ca.crt certs/node.crt certs/node.key /opt/ydb/certs/
  ```

  {% include [prepare-configs.md](_includes/prepare-configs.md) %}

  3\. In the `interconnect_config` and `grpc_config` sections, specify the path to the certificate, key, and CA certificates:

  ```text
    interconnect_config:
        start_tcp: true
        encryption_mode: OPTIONAL
        path_to_certificate_file: "/opt/ydb/certs/node.crt"
        path_to_private_key_file: "/opt/ydb/certs/node.key"
        path_to_ca_file: "/opt/ydb/certs/ca.crt"

    grpc_config:
        cert: "/opt/ydb/certs/node.crt"
        key: "/opt/ydb/certs/node.key"
        ca: "/opt/ydb/certs/ca.crt"
  ```

  Save the configuration file as `/opt/ydb/cfg/config.yaml`

{% endlist %}

## Start static nodes {#start-storage}

{% list tabs %}

- Manual
  1. Run {{ ydb-short-name }} storage on each node:
      ```bash
      sudo su - ydb
      cd /opt/ydb
      export LD_LIBRARY_PATH=/opt/ydb/lib
      /opt/ydb/bin/ydbd server --log-level 3 --syslog --tcp --yaml-config /opt/ydb/cfg/config.yaml \
          --grpc-port 2135 --ic-port 19001 --mon-port 8765 --node static
      ```

- Using systemd
  1. On each node, create a configuration file named `/etc/systemd/system/ydbd-storage.service` with the following content:
      ```text
      [Unit]
      Description=YDB storage node
      After=network-online.target rc-local.service
      Wants=network-online.target
      StartLimitInterval=10
      StartLimitBurst=15

      [Service]
      Restart=always
      RestartSec=1
      User=ydb
      PermissionsStartOnly=true
      StandardOutput=syslog
      StandardError=syslog
      SyslogIdentifier=ydbd
      SyslogFacility=daemon
      SyslogLevel=err
      Environment=LD_LIBRARY_PATH=/opt/ydb/lib
      ExecStart=/opt/ydb/bin/ydbd server --log-level 3 --syslog --tcp \
        --yaml-config /opt/ydb/cfg/config.yaml \
        --grpc-port 2135 --ic-port 19001 --mon-port 8765 --node static
      LimitNOFILE=65536
      LimitCORE=0
      LimitMEMLOCK=3221225472

      [Install]
      WantedBy=multi-user.target
      ```
  2. Run {{ ydb-short-name }} storage on each node:
      ```bash
      sudo systemctl start ydbd-storage
      ```

{% endlist %}

## Initialize the cluster {#initialize-cluster}

On one of the cluster nodes, run the commands:

```bash
export LD_LIBRARY_PATH=/opt/ydb/lib
/opt/ydb/bin/ydbd admin blobstorage config init --yaml-file /opt/ydb/cfg/config.yaml
echo $?
```

The command execution code should be zero.

## Creating the first database {#create-fist-db}

To work with tables, you need to create at least one database and run a process serving this database (a dynamic node).

```bash
export LD_LIBRARY_PATH=/opt/ydb/lib
/opt/ydb/bin/ydbd admin database /Root/testdb create ssd:1
```

## Start the DB dynamic node {#start-dynnode}

{% list tabs %}

- Manual
  1. Start the {{ ydb-short-name }} dynamic node for the /Root/testdb database:
      ```bash
      sudo su - ydb
      cd /opt/ydb
      export LD_LIBRARY_PATH=/opt/ydb/lib
      /opt/ydb/bin/ydbd server --grpc-port 2136 --ic-port 19002 --mon-port 8766 \
        --yaml-config /opt/ydb/cfg/config.yaml --tenant /Root/testdb \
        --node-broker <node1.ydb.tech>:2135 \
        --node-broker <node2.ydb.tech>:2135 \
        --node-broker <node3.ydb.tech>:2135
      ```
      Where `<nodeX.ydb.tech>` is the FQDN of the server running the static nodes.

  2. Run the additional dynamic nodes on other servers to ensure database availability.

- Using systemd
  1. Create a configuration file named `/etc/systemd/system/ydbd-testdb.service` with the following content:
      ```text
      [Unit]
      Description=YDB testdb dynamic node
      After=network-online.target rc-local.service
      Wants=network-online.target
      StartLimitInterval=10
      StartLimitBurst=15

      [Service]
      Restart=always
      RestartSec=1
      User=ydb
      PermissionsStartOnly=true
      StandardOutput=syslog
      StandardError=syslog
      SyslogIdentifier=ydbd
      SyslogFacility=daemon
      SyslogLevel=err
      Environment=LD_LIBRARY_PATH=/opt/ydb/lib
      ExecStart=/opt/ydb/bin/ydbd server --grpc-port 2136 --ic-port 19002 --mon-port 8766 \
          --yaml-config /opt/ydb/cfg/config.yaml --tenant /Root/testdb \
          --node-broker <node1.ydb.tech>:2135 \
          --node-broker <node2.ydb.tech>:2135 \
          --node-broker <node3.ydb.tech>:2135
      LimitNOFILE=65536
      LimitCORE=0
      LimitMEMLOCK=32212254720

      [Install]
      WantedBy=multi-user.target
      ```
      Where `<nodeX.ydb.tech>` is the FQDN of the server running the static nodes.

  2. Start the {{ ydb-short-name }} dynamic node for the /Root/testdb database:
        ```bash
        sudo systemctl start ydbd-testdb
        ```
  3. Run the additional dynamic nodes on other servers to ensure database availability.

{% endlist %}

## Test the created database {#try-first-db}

1. Install the YDB CLI as described in [Installing the YDB CLI](../../reference/ydb-cli/install.md)
2. Create a `test_table`:

```bash
ydb -e grpc://<node.ydb.tech>:2136 -d /Root/testdb scripting yql \
--script 'CREATE TABLE `testdir/test_table` (id Uint64, title Utf8, PRIMARY KEY (id));'
```

Where `<node.ydb.tech>` is the FQDN of the server running any of the dynamic nodes that support the `/Root/testdb` database.
