# Deploying a {{ ydb-short-name }} cluster on virtual or bare-metal servers

This document describes how to deploy a multi-tenant {{ ydb-short-name }} cluster on multiple bare-metal or virtual servers.

## Before you begin {#before-start}

### Prerequisites {#requirements}

Review the [system requirements](../../cluster/system-requirements.md) and the [cluster topology](../../cluster/topology.md).

Make sure you have SSH access to all servers. This is required to install artifacts and run the {{ ydb-short-name }} executable.

The network configuration must allow TCP connections on the following ports (by default, can be changed if necessary):

* 22: SSH service.
* 2135, 2136: GRPC for client-cluster interaction.
* 19001, 19002: Interconnect for intra-cluster node interaction.
* 8765, 8766: The HTTP interface of {{ ydb-short-name }} Embedded UI.

Ensure the clock synchronization for the servers within the cluster, using `ntpd` or `chrony` tools. Ideally all servers should be synced to the same time source, to ensure that leap seconds are handled in the same way.

If your servers' Linux flavor uses `syslogd` for logging, configure logfiles rotation using the `logrotate` or similar tools. {{ ydb-short-name }} services may generate a significant amount of log data, specifically when the logging level is increased for diagnostical purposes, so system log files rotation is important to avoid the overflows of the `/var` filesystem.

Select the servers and disks to be used for storing data:

* Use the `block-4-2` fault tolerance model for cluster deployment in one availability zone (AZ). Use at least 8 nodes to be able to withstand the loss of 2 of them.
* Use the `mirror-3-dc` fault tolerance model for cluster deployment in three availability zones (AZ). To survive the loss of a single AZ and of 1 node in another AZ, use at least 9 nodes. The number of nodes in each AZ should be the same.

{% note info %}

Run each static node on a separate server. Static and dynamic nodes may run on the same server. Multiple dynamic nodes may run on the same server, provided that it has sufficient compute resources.

{% endnote %}

For more information about the hardware requirements, see [{#T}](../../cluster/system-requirements.md).

### TLS keys and certificates preparation {#tls-certificates}

Traffic protection and {{ ydb-short-name }} server node authentication is implemented using the TLS protocol. Before installing the cluster, the list of nodes, their naming scheme and particular names should be defined, and used to prepare the TLS keys and certificates.

The existing or new TLS certificates can be used. The following PEM-encoded key and certificate files are needed to run the cluster:
* `ca.crt` - public certificate of the Certification Authority (CA), used to sign all other TLS certificate (same file on all servers in the cluster);
* `node.key` - secret keys for each of the cluster nodes (separate key for each server);
* `node.crt` - public certificate for each of the cluster nodes (the certificate for the corresponding private key);
* `web.pem` - node secret key, node public certificate and Certification Authority certificate concatenation, to be used by the internal HTTP monitoring service (separate file for each server).

Certificate parameters are typically defined by the organizational policies. Typically {{ ydb-short-name }} certificates are generated with the following parameters:
* 2048 or 4096 bit RSA keys;
* SHA-256 with RSA encryption algorithm for certificate signing;
* node certificates validity period - 1 year;
* CA certificate validity period - 3 years or more.

The CA certificate must be marked appropriately: it needs the CA sign, and the usage for "Digital Signature, Non Repudiation, Key Encipherment, Certificate Sign" enabled.

For node certificates, it is important that the actual host name (or names) matches the values specified in the "Subject Alternative Name" field. Node certificates should have "Digital Signature, Key Encipherment" usage enabled, as well as "TLS Web Server Authentication, TLS Web Client Authentication" extended usage. Node certificates should support both server and client authentication (`extendedKeyUsage = serverAuth,clientAuth` option in the OpenSSL settings).

{{ ydb-short-name }} repository on Github contains the [sample script](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/tls_cert_gen/) which can be used to automate the batch generation or renewal of TLS certificates for the whole cluster. The script can build the key and certificate files for the list of cluster nodes in a single operation, which simplifies the installation preparation.

## Create a system user and a group to run {{ ydb-short-name }} {#create-user}

On each server that will be running {{ ydb-short-name }}, execute the command below:

```bash
sudo groupadd ydb
sudo useradd ydb -g ydb
```

To make sure that {{ ydb-short-name }} has access to block disks to run, the new system user needs to be added to the `disk` group:

```bash
sudo usermod -aG disk ydb
```

## Install {{ ydb-short-name }} software on each server {#install-binaries}

1. Download and unpack the archive with the `ydbd` executable and the required libraries:

   ```bash
   mkdir ydbd-stable-linux-amd64
   curl -L https://binaries.ydb.tech/ydbd-stable-linux-amd64.tar.gz | tar -xz --strip-component=1 -C ydbd-stable-linux-amd64
   ```

1. Create the directories to install the {{ ydb-short-name }} binaries:

   ```bash
   sudo mkdir -p /opt/ydb /opt/ydb/cfg
   ```

1. Copy the executable and libraries to the appropriate directories:

   ```bash
   sudo cp -iR ydbd-stable-linux-amd64/bin /opt/ydb/
   sudo cp -iR ydbd-stable-linux-amd64/lib /opt/ydb/
   ```

1. Set the file and directory ownership:

    ```bash
    sudo chown -R root:bin /opt/ydb
    ```

## Prepare and format disks on each server {#prepare-disks}

{% include [_includes/storage-device-requirements.md](../../_includes/storage-device-requirements.md) %}

1. Create a partition on the selected disk:

   {% note alert %}

   The following step will delete all partitions on the specified disks. Make sure that you specified the disks that have no other data!

   {% endnote %}

   ```bash
    DISK=/dev/nvme0n1
    sudo parted ${DISK} mklabel gpt -s
    sudo parted -a optimal ${DISK} mkpart primary 0% 100%
    sudo parted ${DISK} name 1 ydb_disk_ssd_01
    sudo partx --u ${DISK}
   ```

   As a result, a disk labeled `/dev/disk/by-partlabel/ydb_disk_ssd_01` will appear in the system.

   If you plan to use more than one disk on each server, replace `ydb_disk_ssd_01` with a unique label for each one. Disk labels must be unique within a single server, and are used in the configuration files, as shown in the subsequent instructions.

   For cluster servers having similar disk configuration it is convenient to use exacty the same disk labels, to simplify the subsequent configuration.

2. Format the disk with the builtin command below:

   ```bash
   sudo LD_LIBRARY_PATH=/opt/ydb/lib /opt/ydb/bin/ydbd admin bs disk obliterate /dev/disk/by-partlabel/ydb_disk_ssd_01
   ```

   Perform this operation for each disk that will be used to store {{ ydb-short-name }} data.

## Prepare configuration files {#config}

{% include [prepare-configs.md](_includes/prepare-configs.md) %}

When TLS traffic protection is to be used (which is the default), ensure that {{ ydb-short-name }} configuration file contains the proper paths to key and certificate files in the `interconnect_config` and `grpc_config` sections, as shown below:

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
    services_enabled:
    - legacy
```

Save the {{ ydb-short-name }} configuration file as `/opt/ydb/cfg/config.yaml` on each server of the cluster.

For more detailed information about creating configurations, see [Cluster configurations](../configuration/config.md).

## Copy TLS keys and certificates to each server {#tls-copy-cert}

The TLS keys and certificates prepared need to be copied into the protected directory on each node of the {{ ydb-short-name }} cluster. An example of commands to create of the protected directory and copy the key and certificate files into it is shown below.

```bash
sudo mkdir -p /opt/ydb/certs
sudo cp -v ca.crt /opt/ydb/certs/
sudo cp -v node.crt /opt/ydb/certs/
sudo cp -v node.key /opt/ydb/certs/
sudo cp -v web.pem /opt/ydb/certs/
sudo chown -R ydb:ydb /opt/ydb/certs
sudo chmod 700 /opt/ydb/certs
```

## Start static nodes {#start-storage}

{% list tabs %}

- Manually

   Run {{ ydb-short-name }} storage service on each static node:

   ```bash
   sudo su - ydb
   cd /opt/ydb
   export LD_LIBRARY_PATH=/opt/ydb/lib
   /opt/ydb/bin/ydbd server --log-level 3 --syslog --tcp --yaml-config  /opt/ydb/cfg/config.yaml \
       --grpcs-port 2135 --ic-port 19001 --mon-port 8765 --mon-cert /opt/ydb/certs/web.pem --node static
   ```

- Using systemd

   On each static node, create a `/etc/systemd/system/ydbd-storage.service` systemd configuration file with the following contents. Sample file is also available [in the repository](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/systemd_services/ydbd-storage.service).

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
       --yaml-config  /opt/ydb/cfg/config.yaml \
       --grpcs-port 2135 --ic-port 19001 --mon-port 8765 \
       --mon-cert /opt/ydb/certs/web.pem --node static
   LimitNOFILE=65536
   LimitCORE=0
   LimitMEMLOCK=3221225472

   [Install]
   WantedBy=multi-user.target
   ```

   Run {{ ydb-short-name }} storage service on each static node:

   ```bash
   sudo systemctl start ydbd-storage
   ```

{% endlist %}

## Initialize a cluster {#initialize-cluster}

Cluster initialization configures the set of static nodes defined in the cluster configuration file to store {{ ydb-short-name }} data.

To perform the cluster initialization, the path to the `ca.crt` file containing the Certification Authority certificate has to be specified in the corresponding commands. Copy the `ca.crt` file to the host where those commands will be executed.

Cluster initialization actions sequence depends on whether user authentication mode is enabled in the {{ ydb-short-name }} configuration file.

{% list tabs %}

- Authentication enabled

   To execute the administrative commands (including cluster initialization, database creation, disk management, and others) in a cluster with user authentication enabled, an authentication token has to be obtained using the {{ ydb-short-name }} CLI client version 2.0.0 or higher. The {{ ydb-short-name }} CLI client can be installed on any computer with network access to the cluster nodes (for example, on one of the cluster nodes) by following the [installation instructions](../../reference/ydb-cli/install.md).

   When the cluster is first installed, it has a single `root` account with a blank password, so the command to get the token is the following:

   ```bash
   ydb -e grpcs://<node1.ydb.tech>:2135 -d /Root --ca-file ca.crt \
        --user root --no-password auth get-token --force >token-file
   ```

   Any static node's address can be specified as the endpoint (the `-e` or `--endpoint` parameter).

   If the command above is executed successfully, the authentication token will be written to `token-file`. This token file needs to be copied to one of the cluster storage nodes. Next, run the following commands on this cluster node:

   ```bash
   export LD_LIBRARY_PATH=/opt/ydb/lib
   /opt/ydb/bin/ydbd -f token-file --ca-file ca.crt -s grpcs://`hostname -f`:2135 \
       admin blobstorage config init --yaml-file  /opt/ydb/cfg/config.yaml
   echo $?
   ```

- Authentication disabled

   On one of the cluster storage nodes, run the commands:

   ```bash
   export LD_LIBRARY_PATH=/opt/ydb/lib
   /opt/ydb/bin/ydbd --ca-file ca.crt -s grpcs://`hostname -f`:2135 \
       admin blobstorage config init --yaml-file  /opt/ydb/cfg/config.yaml
   echo $?
   ```

{% endlist %}

Upon successful cluster initialization, the command execution status code shown on the screen should be zero.

## Create a database {#create-db}

To work with tables, you need to create at least one database and run a process (or processes) to service this database (a dynamic node).

In order to run the database creation administrative command, the `ca.crt` file with the CA certificate is needed, similar to the cluster initialization steps shown above.

On database creation the initial number of storage groups is configured, which determines the available input/output throughput and data storage capacity. The number of storage groups can be increased after the database creation, if needed.

Database creation actions sequence depends on whether user authentication mode is enabled in the {{ ydb-short-name }} configuration file.

{% list tabs %}

- Authentication enabled

  The authentication token is needed. The existing token file obtained at [cluster initialization stage](#initialize-cluster) can be used, or the new token can be obtained.

  The authentication token file needs to be copied to one of the static nodes. Next, run the following commands on this cluster node:

  ```bash
  export LD_LIBRARY_PATH=/opt/ydb/lib
  /opt/ydb/bin/ydbd -f token-file --ca-file ca.crt -s grpcs://`hostname -s`:2135 \
      admin database /Root/testdb create ssd:1
  echo $?
  ```

- Authentication disabled

  On one of the static nodes, run the commands:

  ```bash
  export LD_LIBRARY_PATH=/opt/ydb/lib
  /opt/ydb/bin/ydbd --ca-file ca.crt -s grpcs://`hostname -s`:2135 \
      admin database /Root/testdb create ssd:1
  echo $?
  ```

{% endlist %}

The command examples above use the following parameters:
* `/Root`: The name of the root domain, must match the `domains_config`.`domain`.`name` setting in the cluster configuration file.
* `testdb`: The name of the created database.
* `ssd:1`:  The name of the storage pool and the number of the storage groups to be used by the database. The pool name usually means the type of data storage devices and must match the `storage_pool_types`.`kind` setting inside the `domains_config`.`domain` element of the configuration file.

Upon successful database creation, the command execution status code shown on the screen should be zero.

## Start the dynamic nodes {#start-dynnode}

{% list tabs %}

- Manually

   Start the {{ ydb-short-name }} dynamic node for the `/Root/testdb` database:

   ```bash
   sudo su - ydb
   cd /opt/ydb
   export LD_LIBRARY_PATH=/opt/ydb/lib
   /opt/ydb/bin/ydbd server --grpcs-port 2136 --grpc-ca /opt/ydb/certs/ca.crt \
       --ic-port 19002 --ca /opt/ydb/certs/ca.crt \
       --mon-port 8766 --mon-cert /opt/ydb/certs/web.pem \
       --yaml-config  /opt/ydb/cfg/config.yaml --tenant /Root/testdb \
       --node-broker grpcs://<ydb1>:2135 \
       --node-broker grpcs://<ydb2>:2135 \
       --node-broker grpcs://<ydb3>:2135
   ```

   In the command shown above `<ydbN>` entries correspond to the FQDNs of any three servers running the static nodes.

- Using systemd

   Create a systemd configuration file named `/etc/systemd/system/ydbd-testdb.service` with the following content. Sample file is also available [in the repository](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/systemd_services/ydbd-testdb.service).

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
   ExecStart=/opt/ydb/bin/ydbd server \
       --grpcs-port 2136 --grpc-ca /opt/ydb/certs/ca.crt \
       --ic-port 19002 --ca /opt/ydb/certs/ca.crt \
       --mon-port 8766 --mon-cert /opt/ydb/certs/web.pem \
       --yaml-config  /opt/ydb/cfg/config.yaml --tenant /Root/testdb \
       --node-broker grpcs://<ydb1>:2135 \
       --node-broker grpcs://<ydb2>:2135 \
       --node-broker grpcs://<ydb3>:2135
   LimitNOFILE=65536
   LimitCORE=0
   LimitMEMLOCK=32212254720

   [Install]
   WantedBy=multi-user.target
   ```

   In the file shown above `<ydbN>` entries correspond to the FQDNs of any three servers running the static nodes.

   Start the {{ ydb-short-name }} dynamic node for the `/Root/testdb` database:

   ```bash
   sudo systemctl start ydbd-testdb
   ```

{% endlist %}

Start the additional dynamic nodes on other servers to scale and to ensure database and availability.

## Initial user accounts setup {#security-setup}

If authentication mode is enabled in the cluster configuration file, initial user accounts setup must be done before working with the {{ ydb-short-name }} cluster.

The initial installation of the {{ ydb-short-name }} cluster automatically creates a `root` account with a blank password, as well as a standard set of user groups described in the [Access management](../../cluster/access.md) section.

To perform the initial user accounts setup in the created {{ ydb-short-name }} cluster, run the following operations:

1. Install the {{ ydb-short-name }} CLI as described in the [documentation](../../reference/ydb-cli/install.md).

1. Set the password for the `root` account:

   ```bash
   ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root --no-password \
       yql -s 'ALTER USER root PASSWORD "passw0rd"'
   ```

   Replace the `passw0rd` value with the required password.

1. Create the additional accounts:

   ```bash
   ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root \
       yql -s 'CREATE USER user1 PASSWORD "passw0rd"'
   ```

1. Set the account permissions by including it into the security groups:

   ```bash
   ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root \
       yql -s 'ALTER GROUP `ADMINS` ADD USER user1'
   ```

In the command examples above, `<node.ydb.tech>` is the FQDN of the server running the dynamic node that supports the `/Root/testdb` database.

When running the account creation and group assignment commands, the {{ ydb-short-name }} CLI client will request the `root` user's password. You can avoid multiple password entries by creating a connection profile as described in the [{{ ydb-short-name }} CLI documentation](../../reference/ydb-cli/profile/index.md).

## Test the created database {#try-first-db}

1. Install the {{ ydb-short-name }} CLI as described in the [documentation](../../reference/ydb-cli/install.md).

1. Create a `test_table`:

   ```bash
   ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root \
       yql -s 'CREATE TABLE `testdir/test_table` (id Uint64, title Utf8, PRIMARY KEY (id));'
   ```

   Where `<node.ydb.tech>` is the FQDN of the server running the dynamic node that supports the `/Root/testdb` database.

## Validate the access to the embedded UI

To validate the access to {{ ydb-short-name }} embedded UI a Web browser should be used, opening the address `https://<node.ydb.tech>:8765`, where `<node.ydb.tech>` should be replaced with the FQDN of any static node server.

Web browser should be configured to trust the CA used to generate the cluster node certificates, otherwise a warning will be shown that the certificate is not trusted.

In case the authentication is enabled, the Web browser will display the login and password prompt. After entering the correct credentials, the initial {{ ydb-short-name }} embedded UI page will be shown. The available functions and user interface are described in the following document: [{#T}](../../maintenance/embedded_monitoring/index.md).

{% note info %}

Highly available HTTP load balancer, based on `haproxy`, `nginx` or similar software, is typically used to enable access to the {{ ydb-short-name }} embedded UI. The configuration details for HTTP load balancer are out of scope for the basic {{ ydb-short-name }} installation instruction.

{% endnote %}


# Installing {{ ydb-short-name }} in the unprotected mode

{% note warning %}

We DO NOT recommend to run {{ ydb-short-name }} in the unprotected mode for any purpose.

{% endnote %}

The installation procedure described above assumes that {{ ydb-short-name }} runs in its default protected mode.

The unprotected {{ ydb-short-name }} mode is also available, and is intended for internal purposes, mainly for the development and testing of {{ ydb-short-name }} software. When running in the unprotected mode:
* all traffic is passed in the clear text, including the intra-cluster communications and cluster-client communications;
* user authentication is not used (enabling authentication without TLS traffic protection does not make much sense, as login and password are both passed unprotected through the network).

Installing {{ ydb-short-name }} for the unprotected mode is performed according with the general procedure described above, with the exceptions listed below:

1. TLS keys and certificates generation is skipped. No need to copy the key and certificate files to cluster servers.

1. Subsection `security_config` of section `domains_config` is excluded from the configuration file. Sections `interconnect_config` and `grpc_config` are excluded, too.

1. The syntax of commands to start static and dynamic nodes is reduced: the options referring to TLS key and certificate files are excluded, `grpc` protocol name is used instead of `grpcs` for connection points.

1. The step to obtain the authentication token before cluster initialization and database creation is skipped.

1. Cluster initialization is performed with the following command:

    ```bash
    export LD_LIBRARY_PATH=/opt/ydb/lib
    /opt/ydb/bin/ydbd admin blobstorage config init --yaml-file  /opt/ydb/cfg/config.yaml
    echo $?
    ```

1. Database creation is performed with the following command:

    ```bash
    export LD_LIBRARY_PATH=/opt/ydb/lib
    /opt/ydb/bin/ydbd admin database /Root/testdb create ssd:1
    ```

1. `grpc` protocol is used instead of `grpcs` when configuring the connections to the database in {{ ydb-short-name }} CLI and applications. Authentication is not used.
