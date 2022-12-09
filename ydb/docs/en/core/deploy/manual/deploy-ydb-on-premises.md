# Deploying a {{ ydb-short-name }} cluster on virtual or bare-metal servers

This document describes how to deploy a multi-tenant {{ ydb-short-name }} cluster on multiple bare-metal or virtual servers.

## Before you begin {#before-start}

### Prerequisites {#requirements}

Make sure you have SSH access to all servers. This is required to install artifacts and run the {{ ydb-short-name }} executable. The network configuration must allow TCP connections on the following ports (by default):

* 2135, 2136: GRPC for client-cluster interaction.
* 19001, 19002: Interconnect for intra-cluster node interaction.
* 8765, 8766: The HTTP interface for cluster monitoring.

Review the [system requirements](../../cluster/system-requirements.md) and the [cluster topology](../../cluster/topology.md).

Select the servers and disks to be used for storing data:

* Use the `block-4-2` fault tolerance model for cluster deployment in one availability zone (AZ). Use at least 8 nodes to be able to withstand the loss of 2 of them.
* Use the `mirror-3-dc` fault tolerance model for cluster deployment in three availability zones (AZ). To survive the loss of a single AZ and of 1 node in another AZ, use at least 9 nodes. The number of nodes in each AZ should be the same.

Run each static node on a separate server.

For more information about hardware requirements, see [{#T}](../../cluster/system-requirements.md).

## Create a system user and a group to run {{ ydb-short-name }} {#create-user}

On each server that will be running {{ ydb-short-name }}, execute the command below:

```bash
sudo groupadd ydb
sudo useradd ydb -g ydb
```

To make sure that {{ ydb-short-name }} has access to block disks to run, you need to add the process owner to the `disk` group:

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
   sudo mkdir -p /opt/ydb /opt/ydb/cfg
   sudo chown -R ydb:ydb /opt/ydb
   ```

1. Copy the executable and libraries to the appropriate directories:

   ```bash
   sudo cp -iR ydbd-stable-linux-amd64/bin /opt/ydb/
   sudo cp -iR ydbd-stable-linux-amd64/lib /opt/ydb/
   ```

1. Format the disk with the builtin command below:

   ```bash
   sudo LD_LIBRARY_PATH=/opt/ydb/lib /opt/ydb/bin/ydbd admin bs disk obliterate /dev/disk/by-partlabel/ydb_disk_ssd_01
   ```

   Perform this operation for each disk that will be used for data storage.

## Prepare configuration files {#config}

{% list tabs %}

- Unprotected mode

   In unprotected mode, traffic between cluster nodes and between the client and cluster uses an unencrypted connection. Use this mode for testing purposes.

   {% include [prepare-configs.md](_includes/prepare-configs.md) %}

- Protected mode

   In protected mode, traffic between cluster nodes and between the client and cluster is encrypted using the TLS protocol.

   {% note info %}

   You can use existing TLS certificates. It's important that certificates support both server and client authentication (`extendedKeyUsage = serverAuth,clientAuth`).

   {% endnote %}

   1. Create a key and a certificate for the Certification Authority (CA):

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
         openssl req -new -x509 -config ca.cnf -key secure/ca.key -out certs/ca.crt -days 1830 -batch
         ```

      1. Create a text database and an OpenSSL certificate index file:

         ```bash
         touch index.txt
         echo 01 >serial.txt
         ```

   1. Create keys and certificates for the cluster nodes:

      1. Create a `node.cnf` configuration file with the following contents:

         ```text
         # OpenSSL node configuration file
         [ req ]
         prompt = no
         distinguished_name = distinguished_name
         req_extensions = extensions

         [ distinguished_name ]
         organizationName = YDB

         [ extensions ]
         subjectAltName = DNS:<node>.<domain>
         ```

      1. Create a certificate key:

         ```bash
         openssl genrsa -out certs/node.key 2048
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

      1. Copy the certificates and node keys to the installation folder:

         ```bash
         sudo -u ydb cp certs/ca.crt certs/node.crt certs/node.key /opt/ydb/certs/
         ```

   1. {% include [prepare-configs.md](_includes/prepare-configs.md) %}

   1. Enable the traffic encryption mode in the {{ ydb-short-name }} configuration file.

      In the `interconnect_config` and `grpc_config` sections, specify the path to the certificate, key, and CA certificate:

      ```json
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

{% endlist %}

Save the {{ ydb-short-name }} configuration file as `/opt/ydb/cfg/config.yaml` on each cluster node.

For more detailed information about creating configurations, see [Cluster configurations](../configuration/config.md).

## Start static nodes {#start-storage}

{% list tabs %}

- Manually

   Run {{ ydb-short-name }} storage on each node:

   ```bash
   sudo su - ydb
   cd /opt/ydb
   export LD_LIBRARY_PATH=/opt/ydb/lib
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
   Environment=LD_LIBRARY_PATH=/opt/ydb/lib
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

Cluster initialization actions depend on whether user authentication mode is enabled in the {{ ydb-short-name }} configuration file.

{% list tabs %}

- Authentication disabled

   On one of the cluster nodes, run the commands:

   ```bash
   export LD_LIBRARY_PATH=/opt/ydb/lib
   /opt/ydb/bin/ydbd admin blobstorage config init --yaml-file  /opt/ydb/cfg/config.yaml
   echo $?
   ```

   The command execution code should be null.

- Authentication enabled

   To execute administrative commands (including cluster initialization, database creation, disk management, and others) in a cluster with user authentication mode enabled, you must first get an authentication token using the {{ ydb-short-name }} CLI client version 2.0.0 or higher. You must install the {{ ydb-short-name }} CLI client on any computer with network access to the cluster nodes (for example, on one of the cluster nodes) by following the [installation instructions](../../reference/ydb-cli/install.md).

   When the cluster is first installed, it has a single `root` account with a blank password, so the command to get the token is the following:

   ```bash
   ydb -e grpc://<node1.ydb.tech>:2135 -d /Root \
       --user root --no-password auth get-token --force >token-file
   ```

   Any cluster server can be specified as a connection server (the `-e` or `--endpoint` parameter).

   If TLS traffic protection was enabled, use the protected `grpcs` protocol instead of the `grpc` protocol in the command above and additionally specify the path to the CA certificate in the `--ca-file` parameter. For example:

   ```bash
   ydb -e grpcs://<node1.ydb.tech>:2135 -d /Root --ca-file /opt/ydb/certs/ca.crt \
        --user root --no-password auth get-token --force >token-file
   ```

   If the command above is executed successfully, the authentication token will be written to `token-file`. You need to copy this file to the cluster node on which you intend to run the cluster initialization and database creation commands later. Next, run the commands on this cluster node:

   ```bash
   export LD_LIBRARY_PATH=/opt/ydb/lib
   /opt/ydb/bin/ydbd -f token-file admin blobstorage config init --yaml-file  /opt/ydb/cfg/config.yaml
   echo $?
   ```

   The command execution code should be null.

{% endlist %}

## Create a database {#create-db}

To work with tables, you need to create at least one database and run a process to service this database (a dynamic node):

```bash
LD_LIBRARY_PATH=/opt/ydb/lib /opt/ydb/bin/ydbd admin database /Root/testdb create ssd:1
```

If user authentication mode is enabled in the cluster, the authentication token must be passed to the database creation command. The procedure for getting a token is described in the [cluster initialization](#initialize-cluster) section.

A variant of the database creation command with reference to the token file:

```bash
LD_LIBRARY_PATH=/opt/ydb/lib /opt/ydb/bin/ydbd -f token-file admin database /Root/testdb create ssd:1
```

The command examples above use the following parameters:
* `/Root`: The name of the root domain, must match the `domains_config`.`domain`.`name` setting in the cluster configuration file.
* `testdb`: The name of the created database.
* `ssd:1`:  The name of the storage pool and the number of the block in the pool. The pool name usually means the type of data storage devices and must match the `storage_pool_types`.`kind` setting inside the `domains_config`.`domain` element of the configuration file.

## Start the database dynamic node {#start-dynnode}

{% list tabs %}

- Manually

   Start the {{ ydb-short-name }} dynamic node for the /Root/testdb database:

   ```bash
   sudo su - ydb
   cd /opt/ydb
   export LD_LIBRARY_PATH=/opt/ydb/lib
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
   Environment=LD_LIBRARY_PATH=/opt/ydb/lib
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

## Initial account setup {#security-setup}

If authentication mode is enabled in the cluster configuration file, initial account setup must be done before working with the {{ ydb-short-name }} cluster.

The initial installation of the {{ ydb-short-name }} cluster automatically creates a `root` account with a blank password, as well as a standard set of user groups described in the [Access management](../../cluster/access.md) section.

To perform initial account setup in the created {{ ydb-short-name }} cluster, run the following operations:

1. Install the {{ ydb-short-name }} CLI as described in the [documentation](../../reference/ydb-cli/install.md).

1. Set the password for the `root` account:

   ```bash
   ydb -e grpc://<node.ydb.tech>:2136 -d /Root/testdb --user root --no-password \
       yql -s 'ALTER USER root PASSWORD "passw0rd"'
   ```

   Replace the `passw0rd` value with the required password.

1. Create additional accounts:

   ```bash
   ydb -e grpc://<node.ydb.tech>:2136 -d /Root/testdb --user root \
       yql -s 'CREATE USER user1 PASSWORD "passw0rd"'
   ```

1. Set the account rights by including them in the integrated groups:

   ```bash
   ydb -e grpc://<node.ydb.tech>:2136 -d /Root/testdb --user root \
       yql -s 'ALTER GROUP `ADMINS` ADD USER user1'
   ```

In the command examples above, `<node.ydb.tech>` is the FQDN of the server running the dynamic node that supports the `/Root/testdb` database.

When running the account creation and group assignment commands, the {{ ydb-short-name }} CLI client will request the `root` user's password. You can avoid multiple password entries by creating a connection profile as described in the [{{ ydb-short-name }} CLI documentation](../../reference/ydb-cli/profile/index.md).

If TLS traffic protection was enabled in the cluster, use the protected `grpcs` protocol instead of the `grpc` protocol in the command above and specify the path to the CA certificate in the `--ca-file` parameter (or save it in the connection profile).

## Test the created database {#try-first-db}

1. Install the {{ ydb-short-name }} CLI as described in the [documentation](../../reference/ydb-cli/install.md).

1. Create a `test_table`:

   ```bash
   ydb -e grpc://<node.ydb.tech>:2136 -d /Root/testdb scripting yql \
   --script 'CREATE TABLE `testdir/test_table` (id Uint64, title Utf8, PRIMARY KEY (id));'
   ```

   Where `<node.ydb.tech>` is the FQDN of the server running the dynamic node that supports the `/Root/testdb` database.

   The command above must be adjusted if TLS traffic protection or user authentication mode is enabled in the cluster. Example:

   ```bash
   ydb -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --ca-file ydb-ca.crt --user root scripting yql \
   --script 'CREATE TABLE `testdir/test_table` (id Uint64, title Utf8, PRIMARY KEY (id));'
   ```
