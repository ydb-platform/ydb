# Deploying a {{ ydb-short-name }} cluster on virtual or bare-metal servers

This document describes how to deploy a multi-tenant {{ ydb-short-name }} cluster on multiple bare-metal or virtual servers.

## Getting started {#before-start}

### Prerequisites {#requirements}

Review the [system requirements](../../devops/system-requirements.md) and the [cluster topology](../../concepts/topology.md).

Make sure you have SSH access to all servers. This is required to install artifacts and run the {{ ydb-short-name }} executable.

The network configuration must allow TCP connections on the following ports (these are defaults, but you can change them by settings):

* 22: SSH service
* 2135, 2136: GRPC for client-cluster interaction.
* 19001, 19002: Interconnect for intra-cluster node interaction
* 8765, 8766: HTTP interface of {{ ydb-short-name }} Embedded UI.

Distinct ports are necessary for gRPC, Interconnect and HTTP interface of each dynamic node when hosting multiple dynamic nodes on a single server.

Make sure that the system clocks running on all the cluster's servers are synced by `ntpd` or `chrony`. We recommend using the same time source for all servers in the cluster to maintain consistent leap seconds processing.

If the Linux flavor run on the cluster servers uses `syslogd` for logging, set up log file rotation using`logrotate` or similar tools. {{ ydb-short-name }} services can generate substantial amounts of system logs, particularly when you elevate the logging level for diagnostic purposes. That's why it's important to enable system log file rotation to prevent the `/var` file system overflow.

Select the servers and disks to be used for storing data:

* Use the `block-4-2` fault tolerance model for cluster deployment in one availability zone (AZ). Use at least eight servers to safely survive the loss of two servers.
* Use the `mirror-3-dc` fault tolerance model for cluster deployment in three availability zones (AZ). To survive the loss of one AZ and one server in another AZ, use at least nine servers. Make sure that the number of servers running in each AZ is the same.

{% note info %}

Run each static node (data node) on a separate server. Both static and dynamic nodes can run together on the same server. A server can also run multiple dynamic nodes if it has enough computing power.

{% endnote %}

For more information about hardware requirements, see [{#T}](../../devops/system-requirements.md).

### Preparing TLS keys and certificates {#tls-certificates}

The TLS protocol provides traffic protection and authentication for {{ ydb-short-name }} server nodes. Before you install your cluster, determine which servers it will host, establish the node naming convention, come up with node names, and prepare your TLS keys and certificates.

You can use existing certificates or generate new ones. Prepare the following files with TLS keys and certificates in the PEM format:
* `ca.crt`: CA-issued certificate used to sign the other TLS certificates (these files are the same on all the cluster nodes).
* `node.key`: Secret TLS keys for each cluster node (one key per cluster server).
* `node.crt`: TLS certificates for each cluster node (each certificate corresponds to a key).
* `web.pem`: Concatenation of the node secret key, node certificate, and the CA certificate needed for the monitoring HTTP interface (a separate file is used for each server in the cluster).

Your organization should define the parameters required for certificate generation in its policy. The following parameters are commonly used for generating certificates and keys for {{ ydb-short-name }}:
* 2048-bit or 4096-bit RSA keys
* Certificate signing algorithm: SHA-256 with RSA encryption
* Validity period of node certificates: at least 1 year
* CA certificate validity period: at least 3 years.

Make sure that the CA certificate is appropriately labeled, with the CA property enabled along with the "Digital Signature, Non Repudiation, Key Encipherment, Certificate Sign" usage types.

For node certificates, it's key that the actual host name (or names) match the values in the "Subject Alternative Name" field. Enable both the regular usage types ("Digital Signature, Key Encipherment") and advanced usage types ("TLS Web Server Authentication, TLS Web Client Authentication") for the certificates. Node certificates must support both server authentication and client authentication (the `extendedKeyUsage = serverAuth,clientAuth` option in the OpenSSL settings).

For batch generation or update of {{ ydb-short-name }} cluster certificates by OpenSSL, you can use the [sample script](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/tls_cert_gen/) from the {{ ydb-short-name }} GitHub repository. Using the script, you can streamline preparation for installation, automatically generating all the key files and certificate files for all your cluster nodes in a single step.

## Create a system user and a group to run {{ ydb-short-name }} {#create-user}

On each server that will be running {{ ydb-short-name }}, execute the command below:

```bash
sudo groupadd ydb
sudo useradd ydb -g ydb
```

To ensure that {{ ydb-short-name }} can access block disks, add the user that will run {{ ydb-short-name }} processes, to the `disk` group:

```bash
sudo usermod -aG disk ydb
```

## Install {{ ydb-short-name }} software on each {#install-binaries} server

1. Download and unpack an archive with the `ydbd` executable and the libraries required for {{ ydb-short-name }} to run:

   ```bash
   mkdir ydbd-stable-linux-amd64
   curl -L {{ ydb-binaries-url }}/{{ ydb-stable-binary-archive }} | tar -xz --strip-component=1 -C ydbd-stable-linux-amd64
   ```

1. Create directories for {{ ydb-short-name }} software:

   ```bash
   sudo mkdir -p /opt/ydb /opt/ydb/cfg
   ```

1. Copy the executable and libraries to the appropriate directories:

   ```bash
   sudo cp -iR ydbd-stable-linux-amd64/bin /opt/ydb/
   sudo cp -iR ydbd-stable-linux-amd64/lib /opt/ydb/
   ```

1. Set the owner of files and folders:

   ```bash
   sudo chown -R root:bin /opt/ydb
   ```

## Prepare and format disks on each server {#prepare-disks}

{% include [_includes/storage-device-requirements.md](../../_includes/storage-device-requirements.md) %}

1. Create partitions on the selected disks:

   {% note alert %}

   The next operation will delete all partitions on the specified disk. Make sure that you specified a disk that contains no external data.

   {% endnote %}

   ```bash
   DISK=/dev/nvme0n1
   sudo parted ${DISK} mklabel gpt -s
   sudo parted -a optimal ${DISK} mkpart primary 0% 100%
   sudo parted ${DISK} name 1 ydb_disk_ssd_01
   sudo partx --u ${DISK}
   ```

   As a result, a disk labeled `/dev/disk/by-partlabel/ydb_disk_ssd_01` will appear on the system.

   If you plan to use more than one disk on each server, replace `ydb_disk_ssd_01` with a unique label for each one. Disk labels should be unique within each server. They are used in configuration files, see the following guides.

   To streamline the next setup step, it makes sense to use the same disk labels on cluster servers having the same disk configuration.

2. Format the disk by this command built-in the `ydbd` executable:

   ```bash
   sudo LD_LIBRARY_PATH=/opt/ydb/lib /opt/ydb/bin/ydbd admin bs disk obliterate /dev/disk/by-partlabel/ydb_disk_ssd_01
   ```

   Perform this operation for each disk to be used for {{ ydb-short-name }} data storage.

## Prepare configuration files {#config}

{% include [prepare-configs.md](_includes/prepare-configs.md) %}

In the traffic encryption mode, make sure that the {{ ydb-short-name }} configuration file specifies paths to key files and certificate files under `interconnect_config` and `grpc_config`:

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

Save the {{ ydb-short-name }} configuration file as `/opt/ydb/cfg/config.yaml` on each cluster node.

For more detailed information about creating the configuration file, see [Cluster configurations](../configuration/config.md).

## Copy the TLS keys and certificates to each server {#tls-copy-cert}

Make sure to copy the generated TLS keys and certificates to a protected folder on each {{ ydb-short-name }} cluster node. Below are sample commands that create a protected folder and copy files with keys and certificates.

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

   Run a {{ ydb-short-name }} data storage service on each static cluster node:

   ```bash
   sudo su - ydb
   cd /opt/ydb
   export LD_LIBRARY_PATH=/opt/ydb/lib
   /opt/ydb/bin/ydbd server --log-level 3 --syslog --tcp --yaml-config  /opt/ydb/cfg/config.yaml \
       --grpcs-port 2135 --ic-port 19001 --mon-port 8765 --mon-cert /opt/ydb/certs/web.pem --node static
   ```

- Using systemd

   On each server that will host a static cluster node, create a systemd `/etc/systemd/system/ydbd-storage.service` configuration file by the template below. You can also [download](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/systemd_services/ydbd-storage.service) the sample file from the repository.

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

   Run the service on each static {{ ydb-short-name }} node:

   ```bash
   sudo systemctl start ydbd-storage
   ```

{% endlist %}

## Initialize a cluster {#initialize-cluster}

The cluster initialization operation sets up static nodes listed in the cluster configuration file, for storing {{ ydb-short-name }} data.

To initialize the cluster, you'll need the `ca.crt` file issued by the Certificate Authority. Use its path in the initialization commands. Before running the commands, copy `ca.crt` to the server where you will run the commands.

Cluster initialization actions depend on whether the user authentication mode is enabled in the {{ ydb-short-name }} configuration file.

{% list tabs %}

- Authentication enabled

   To execute administrative commands (including cluster initialization, database creation, disk management, and others) in a cluster with user authentication mode enabled, you must first get an authentication token using the {{ ydb-short-name }} CLI client version 2.0.0 or higher. You must install the {{ ydb-short-name }} CLI client on any computer with network access to the cluster nodes (for example, on one of the cluster nodes) by following the [installation instructions](../../reference/ydb-cli/install.md).

   When the cluster is first installed, it has a single `root` account with a blank password, so the command to get the token is the following:

   ```bash
   ydb -e grpcs://<node1.ydb.tech>:2135 -d /Root --ca-file ca.crt \
        --user root --no-password auth get-token --force >token-file
   ```

   You can specify any storage server in the cluster as an endpoint (the `-e` or `--endpoint` parameter).

   If the command above is executed successfully, the authentication token will be written to `token-file`. Copy the token file to one of the storage servers in the cluster, then run the following commands on the server:

   ```bash
   export LD_LIBRARY_PATH=/opt/ydb/lib
   /opt/ydb/bin/ydbd -f token-file --ca-file ca.crt -s grpcs://`hostname -f`:2135 \
       admin blobstorage config init --yaml-file  /opt/ydb/cfg/config.yaml
   echo $?
   ```

- Authentication disabled

   On one of the storage servers in the cluster, run these commands:

   ```bash
   export LD_LIBRARY_PATH=/opt/ydb/lib
   /opt/ydb/bin/ydbd --ca-file ca.crt -s grpcs://`hostname -f`:2135 \
       admin blobstorage config init --yaml-file  /opt/ydb/cfg/config.yaml
   echo $?
   ```

{% endlist %}

You will see that the cluster was initialized successfully when the cluster initialization command returns a zero code.

## Create a database {#create-db}

To work with [row](../../concepts/datamodel/table.md#row-oriented-tables) and [column](../../concepts/datamodel/table.md#column-oriented-tables) oriented tables, you need to create at least one database and run a process (or processes) to serve this database (dynamic nodes):

To execute the administrative command for database creation, you will need the `ca.crt` certificate file issued by the Certificate Authority (see the above description of cluster initialization).

When creating your database, you set an initial number of storage groups that determine the available input/output throughput and maximum storage. For an existing database, you can increase the number of storage groups when needed.

The database creation procedure depends on whether you enabled user authentication in the {{ ydb-short-name }} configuration file.

{% list tabs %}

- Authentication enabled

   Get an authentication token. Use the authentication token file that you obtained when [initializing the cluster](#initialize-cluster) or generate a new token.

   Copy the token file to one of the storage servers in the cluster, then run the following commands on the server:

   ```bash
   export LD_LIBRARY_PATH=/opt/ydb/lib
   /opt/ydb/bin/ydbd -f token-file --ca-file ca.crt -s grpcs://`hostname -s`:2135 \
       admin database /Root/testdb create ssd:1
   echo $?
   ```

- Authentication disabled

   On one of the storage servers in the cluster, run these commands:

   ```bash
   export LD_LIBRARY_PATH=/opt/ydb/lib
   /opt/ydb/bin/ydbd --ca-file ca.crt -s grpcs://`hostname -s`:2135 \
       admin database /Root/testdb create ssd:1
   echo $?
   ```

{% endlist %}

You will see that the database was created successfully when the command returns a zero code.

The command example above uses the following parameters:
* `/Root`: Name of the root domain, must match the `domains_config`.`domain`.`name` setting in the cluster configuration file.
* `testdb`: Name of the created database.
* `ssd:1`:  Name of the storage pool and the number of storage groups allocated. The pool name usually means the type of data storage devices and must match the `storage_pool_types`.`kind` setting inside the `domains_config`.`domain` element of the configuration file.

## Run dynamic nodes {#start-dynnode}

{% list tabs %}

- Manually

   Run the {{ ydb-short-name }} dynamic node for the `/Root/testdb` database:

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

   In the command example above, `<ydbN>` is replaced by FQDNs of any three servers running the cluster's static nodes.

- Using systemd

   Create a systemd configuration file named `/etc/systemd/system/ydbd-testdb.service` by the following template: You can also [download](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/systemd_services/ydbd-testdb.service) the sample file from the repository.

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

   In the file example above, `<ydbN>` is replaced by FQDNs of any three servers running the cluster's static nodes.

   Run the {{ ydb-short-name }} dynamic node for the `/Root/testdb` database:

   ```bash
   sudo systemctl start ydbd-testdb
   ```

{% endlist %}

Run additional dynamic nodes on other servers to ensure database scalability and fault tolerance.

## Initial account setup {#security-setup}

If authentication mode is enabled in the cluster configuration file, initial account setup must be done before working with the {{ ydb-short-name }} cluster.

The initial installation of the {{ ydb-short-name }} cluster automatically creates a `root` account with a blank password, as well as a standard set of user groups described in the [Access management](../../security/access-management.md) section.

To perform initial account setup in the created {{ ydb-short-name }} cluster, run the following operations:

1. Install the {{ ydb-short-name }} CLI as described in the [documentation](../../reference/ydb-cli/install.md).

1. Set the password for the `root` account:

   ```bash
   ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root --no-password \
       yql -s 'ALTER USER root PASSWORD "passw0rd"'
   ```

   Replace the `passw0rd` value with the required password.

1. Create additional accounts:

   ```bash
   ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root \
       yql -s 'CREATE USER user1 PASSWORD "passw0rd"'
   ```

1. Set the account rights by including them in the integrated groups:

   ```bash
   ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root \
       yql -s 'ALTER GROUP `ADMINS` ADD USER user1'
   ```

In the command examples above, `<node.ydb.tech>` is the FQDN of the server running any dynamic node that serves the `/Root/testdb` database.

When running the account creation and group assignment commands, the {{ ydb-short-name }} CLI client will request the `root` user's password. You can avoid multiple password entries by creating a connection profile as described in the [{{ ydb-short-name }} CLI documentation](../../reference/ydb-cli/profile/index.md).

## Test the created database {#try-first-db}

1. Install the {{ ydb-short-name }} CLI as described in the [documentation](../../reference/ydb-cli/install.md).

1. Create a testing row (`test_row_table`) or column (`test_column_table`) oriented table:

{% list tabs %}

- Creating a row oriented table
   ```bash
   ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root \
      yql -s 'CREATE TABLE `testdir/test_row_table` (id Uint64, title Utf8, PRIMARY KEY (id));'
   ```

- Creating a column oriented table
   ```bash
   ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root \
      yql -s 'CREATE TABLE `testdir/test_column_table` (id Uint64, title Utf8, PRIMARY KEY (id)) WITH (STORE = COLUMN);'
   ```

{% endlist %}

Here, `<node.ydb.tech>` is the FQDN of the server running the dynamic node that serves the `/Root/testdb` database.

## Checking access to the built-in web interface

To check access to the {{ ydb-short-name }} built-in web interface, open in the browser the `https://<node.ydb.tech>:8765` URL, where `<node.ydb.tech>` is the FQDN of the server running any static {{ ydb-short-name }} node.

In the web browser, set as trusted the certificate authority that issued certificates for the {{ ydb-short-name }} cluster. Otherwise, you will see a warning about an untrusted certificate.

If authentication is enabled in the cluster, the web browser should prompt you for a login and password. Enter your credentials, and you'll see the built-in interface welcome page. The user interface and its features are described in [{#T}](../../reference/embedded-ui/index.md).

{% note info %}

A common way to provide access to the {{ ydb-short-name }} built-in web interface is to set up a fault-tolerant HTTP balancer running `haproxy`, `nginx`, or similar software. A detailed description of the HTTP balancer is beyond the scope of the standard {{ ydb-short-name }} installation guide.

{% endnote %}


# Installing {{ ydb-short-name }} in the unprotected mode

{% note warning %}

We do not recommend using the unprotected {{ ydb-short-name }} mode for development or production environments.

{% endnote %}

The above installation procedure assumes that {{ ydb-short-name }} was deployed in the standard protected mode.

The unprotected {{ ydb-short-name }} mode is primarily intended for test scenarios associated with {{ ydb-short-name }} software development and testing. In the unprotected mode:
* Traffic between cluster nodes and between applications and the cluster runs over an unencrypted connection.
* Users are not authenticated (it doesn't make sense to enable authentication when the traffic is unencrypted because the login and password in such a configuration would be transparently transmitted across the network).

When installing {{ ydb-short-name }} to run in the unprotected mode, follow the above procedure, with the following exceptions:

1. When preparing for the installation, you do not need to generate TLS certificates and keys and copy the certificates and keys to the cluster nodes.

1. In the configuration files, remove the `security_config` subsection under `domains_config`. Remove the `interconnect_config` and `grpc_config` sections entirely.

1. Use simplified commands to run static and dynamic cluster nodes: omit the options that specify file names for certificates and keys; use the `grpc` protocol instead of `grpcs` when specifying the connection points.

1. Skip the step of obtaining an authentication token before cluster initialization and database creation because it's not needed in the unprotected mode.

1. Cluster initialization command has the following format:

   ```bash
   export LD_LIBRARY_PATH=/opt/ydb/lib
   /opt/ydb/bin/ydbd admin blobstorage config init --yaml-file  /opt/ydb/cfg/config.yaml
   echo $?
   ```

1. Database creation command has the following format:

   ```bash
   export LD_LIBRARY_PATH=/opt/ydb/lib
   /opt/ydb/bin/ydbd admin database /Root/testdb create ssd:1
   ```

1. When accessing your database from the {{ ydb-short-name }} CLI and applications, use grpc instead of grpcs and skip authentication.
