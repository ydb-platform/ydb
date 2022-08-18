# Deploying a {{ ydb-short-name }} cluster on virtual or bare metal servers

This document describes the procedure to deploy a multi-tenant {{ ydb-short-name }} cluster on several servers.

## Before you begin {#before-start}

### Prerequisites {#requirements}

Make sure you have SSH access to all servers. This is required to install artifacts and run the {{ ydb-short-name }} binary. Your network configuration must allow TCP connections on the following ports (by default):

* 2135, 2136: GRPC for client-cluster interaction.
* 19001, 19002: Interconnect for intra-cluster node interaction.
* 8765, 8766: The HTTP interface for cluster monitoring.

Review the [system requirements](../../cluster/system-requirements.md) and the [cluster topology](../../cluster/topology.md).

Select the servers and disks to be used for storing data:

* Use the `block-4-2 ` fault tolerance model for cluster deployment in one availability zone (AZ). Use at least 8 nodes to be able to withstand the loss of 2 of them.
* Use the `mirror-3-dc` fault tolerance model for cluster deployment in three availability zones (AZ). To survive the loss of a single AZ and of 1 node in another AZ, use at least 9 nodes. The number of nodes in each AZ should be the same.

Run each static node on a separate server.

For more information about hardware requirements, see [{#T}](../../cluster/system-requirements.md).

## Create a system user and a group to run {{ ydb-short-name }} under {#create-user}

On each server that will be running {{ ydb-short-name }}, execute the command below:

```bash
sudo groupadd ydb
sudo useradd ydb -g ydb
```

To make sure the {{ ydb-short-name }} server has access to block disks to run, you need to add the process owner to the disk group:

```bash
sudo usermod -aG disk ydb
```

## Prepare and format disks on each server {#prepare-disks}

{% note warning %}

We don't recommend storing data on disks used by other processes (including the operating system).

{% endnote %}

{% include [_includes/storage-device-requirements.md](../../_includes/storage-device-requirements.md) %}

1. Create a partition on the selected disk:

   {% note alert %}

   The following step will delete all partitions on the specified disks. Make sure that you specified the disks that have no other data!

   {% endnote %}

   ```bash
   sudo parted /dev/nvme0n1 mklabel gpt -s
   sudo parted -a optimal /dev/nvme0n1 mkpart primary 0% 100%
   sudo parted /dev/nvme0n1 name 1 ydb_disk_ssd_01
   sudo partx --u /dev/nvme0n1
   ```

   As a result, a disk labeled `/dev/disk/by-partlabel/ydb_disk_ssd_01` will appear on the system.

   If you plan to use more than one disk on each server, replace `ydb_disk_ssd_01` with a unique label for each one. You'll need to use these disks later in the configuration files.

1. Download and unpack an archive with the `ydbd` executable and the libraries required for {{ ydb-short-name }} to run:

   ```bash
   mkdir ydbd-stable-linux-amd64
   curl -L https://binaries.ydb.tech/ydbd-stable-linux-amd64.tar.gz | tar -xz --strip-component=1 -C ydbd-stable-linux-amd64
   ```

1. Create directories to run:

   ```bash
   sudo mkdir -p /opt/ydb/bin /opt/ydb/cfg /opt/ydb/lib
   sudo chown -R ydb:ydb /opt/ydb
   ```

1. Copy the binary file, libraries, and configuration file to the appropriate directories:

   ```bash
   sudo cp -i ydbd-stable-linux-amd64/bin/ydbd /opt/ydb/bin/
   sudo cp -i ydbd-stable-linux-amd64/lib/libaio.so /opt/ydb/lib/
   sudo cp -i ydbd-stable-linux-amd64/lib/libiconv.so /opt/ydb/lib/
   sudo cp -i ydbd-stable-linux-amd64/lib/libidn.so /opt/ydb/lib/
   ```

1. Format the disk with the builtin command below:

   ```bash
   sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/ydb/lib /opt/ydb/bin/ydbd admin bs disk obliterate /dev/disk/by-partlabel/ydb_disk_ssd_01
   ```

   Perform this operation for each disk that will be used for data storage.

## Prepare configuration files {#config}

{% list tabs %}

- Unprotected mode

   In this mode, traffic between cluster nodes and between client and cluster uses an unencrypted connection. Use this mode for testing purposes.

   {% include [prepare-configs.md](_includes/prepare-configs.md) %}

   Save the {{ ydb-short-name }} configuration file as `/opt/ydb/cfg/config.yaml`.

- Protected mode

   In this mode, traffic between cluster nodes and between the client and cluster is encrypted using the TLS protocol.

   {% note info %}

   You can use existing TLS certificates. It's important that certificates support both server and client authentication (`extendedKeyUsage = serverAuth,clientAuth`).

   {% endnote %}

   1. Create a CA key:

      1. Create a directory named `secure` to store the CA key and one named `certs` for certificates and node keys:

         ```bash
         mkdir secure
         mkdir certs
         ```

      1. Create a configuration file named `ca.cnf` with the following contents:

         ```text
         [ ca ]
         default_ca = CA_default

         [ CA_default ]
         default_days = 365
         database = index.txt
         serial = serial.txt
         default_md = sha256
         copy_extensions = copy
         unique_subject = no

         [ req ]
         prompt=no
         distinguished_name = distinguished_name
         x509_extensions = extensions

         [ distinguished_name ]
         organizationName = YDB
         commonName = YDB CA

         [ extensions ]
         keyUsage = critical,digitalSignature,nonRepudiation,keyEncipherment,keyCertSign
         basicConstraints = critical,CA:true,pathlen:1

         [ signing_policy ]
         organizationName = supplied
         commonName = optional

         [ signing_node_req ]
         keyUsage = critical,digitalSignature,keyEncipherment
         extendedKeyUsage = serverAuth,clientAuth

         # Used to sign client certificates.
         [ signing_client_req ]
         keyUsage = critical,digitalSignature,keyEncipherment
         extendedKeyUsage = clientAuth
         ```

      1. Create a CA key:

         ```bash
         openssl genrsa -out secure/ca.key 2048
         ```

         Save this key separately, you'll need it for issuing certificates. If it's lost, you'll have to reissue all certificates.

      1. Create a private Certificate Authority (CA) certificate:

         ```bash
         openssl req -new -x509 -config ca.cnf -key secure/ca.key -out ca.crt -days 365 -batch
         ```

   1. Create keys and certificates for the cluster nodes:

      1. Create a `node.conf` configuration file with the following contents:

         ```text
         # OpenSSL node configuration file
         [ req ]
         prompt=no
         distinguished_name = distinguished_name
         req_extensions = extensions

         [ distinguished_name ]
         organizationName = YDB

         [ extensions ]
         subjectAltName = DNS:<node>.<domain>
         ```

      1. Create a certificate key:

         ```bash
         openssl genrsa -out node.key 2048
         ```

      1. Create a Certificate Signing Request (CSR):

         ```bash
         openssl req -new -sha256 -config node.cnf -key certs/node.key -out node.csr -batch
         ```

      1. Create a node certificate:

         ```bash
         openssl ca -config ca.cnf -keyfile secure/ca.key -cert certs/ca.crt -policy signing_policy \
         -extensions signing_node_req -out certs/node.crt -outdir certs/ -in node.csr -batch
         ```

         Create similar certificate-key pairs for each node.

      1. Create certificate directories on each node:

         ```bash
         sudo mkdir /opt/ydb/certs
         sudo chown -R ydb:ydb /opt/ydb/certs
         sudo chmod 0750 /opt/ydb/certs
         ```

      1. Copy the node certificates and keys

         ```bash
         sudo -u ydb cp certs/ca.crt certs/node.crt certs/node.key /opt/ydb/certs/
         ```

   1. {% include [prepare-configs.md](_includes/prepare-configs.md) %}

      1. In the `interconnect_config` and `grpc_config` sections, specify the path to the certificate, key, and CA certificates:

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

   Save the {{ ydb-short-name }} configuration file as `/opt/ydb/cfg/config.yaml`.

{% endlist %}

## Start static nodes {# start-storage}

{% list tabs %}

- Manually

   Run {{ ydb-short-name }} storage on each node:

   ```bash
   sudo su - ydb
   cd /opt/ydb
   export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/ydb/lib
   /opt/ydb/bin/ydbd server --log-level 3 --syslog --tcp --yaml-config /opt/ydb/cfg/config.yaml  \
   --grpc-port 2135 --ic-port 19001 --mon-port 8765 --node static
   ```

- Using systemd

   On every node, create a `/etc/systemd/system/ydbd-storage.service` configuration file with the following contents:

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
   Environment=LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/ydb/lib
   ExecStart=/opt/ydb/bin/ydbd server --log-level 3 --syslog --tcp --yaml-config  /opt/ydb/cfg/config.yaml --grpc-port 2135 --ic-port 19001 --mon-port 8765 --node static
   LimitNOFILE=65536
   LimitCORE=0
   LimitMEMLOCK=3221225472

   [Install]
   WantedBy=multi-user.target
   ```

   Run {{ ydb-short-name }} storage on each node:

   ```bash
   sudo systemctl start ydbd-storage
   ```

{% endlist %}

## Initialize a cluster {#initialize-cluster}

On one of the cluster nodes, run the command:

```bash
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/ydb/lib /opt/ydb/bin/ydbd admin blobstorage config init --yaml-file  /opt/ydb/cfg/config.yaml ; echo $?
```

The command execution code should be null.

## Creating the first database {#create-fist-db}

To work with tables, you need to create at least one database and run a process to service this database (a dynamic node):

```bash
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/ydb/lib /opt/ydb/bin/ydbd admin database /Root/testdb create ssd:1
```

## Start the DB dynamic node {#start-dynnode}

{% list tabs %}

- Manually

   Start the {{ ydb-short-name }} dynamic node for the /Root/testdb database:

   ```bash
   sudo su - ydb
   cd /opt/ydb
   export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/ydb/lib
   /opt/ydb/bin/ydbd server --grpc-port 2136 --ic-port 19002 --mon-port 8766 --yaml-config  /opt/ydb/cfg/config.yaml \
   --tenant /Root/testdb --node-broker <node1.ydb.tech>:2135 --node-broker <node2.ydb.tech>:2135 --node-broker <node3.ydb.tech>:2135
   ```

   Where `<nodeN.ydb.tech>` is the FQDN of the servers running the static nodes.

   Run additional dynamic nodes on other servers to ensure database availability.

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
   Environment=LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/ydb/lib
   ExecStart=/opt/ydb/bin/ydbd server --grpc-port 2136 --ic-port 19002 --mon-port 8766 --yaml-config  /opt/ydb/cfg/config.yaml --tenant /Root/testdb --node-broker <node1.ydb.tech>:2135 --node-broker <node2.ydb.tech>:2135 --node-broker <node3.ydb.tech>:2135
   LimitNOFILE=65536
   LimitCORE=0
   LimitMEMLOCK=32212254720

   [Install]
   WantedBy=multi-user.target
   ```

   Where `<nodeN.ydb.tech>` is the FQDN of the servers running the static nodes.

   1. Start the {{ ydb-short-name }} dynamic node for the /Root/testdb database:

   ```bash
   sudo systemctl start ydbd-testdb
   ```

   1. Run additional dynamic nodes on other servers to ensure database availability.

{% endlist %}

## Test the created database {#try-first-db}

1. Install the {{ ydb-short-name }} CLI as described in [Installing the {{ ydb-short-name }} CLI](../../reference/ydb-cli/install.md):

1. Create a `test_table`:

   ```bash
   ydb -e grpc://<node.ydb.tech>:2136 -d /Root/testdb scripting yql \
   --script 'CREATE TABLE `testdir/test_table` (id Uint64, title Utf8, PRIMARY KEY (id));'
   ```

   Where `<node.ydb.tech>` is the FQDN of the server running the dynamic node that supports the `/Root/testdb` database.
