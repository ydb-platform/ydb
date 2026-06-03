# Manual deployment of a {{ ydb-short-name }} cluster

<!-- markdownlint-disable blanks-around-fences -->

{% note warning %}

This guide is intended only for deploying clusters with [V1 configuration](../../configuration-management/configuration-v1/index.md). Deploying clusters with [V2 configuration](../../configuration-management/configuration-v2/index.md) is currently under development.

{% endnote %}

This document describes how to deploy a multi-tenant {{ ydb-short-name }} cluster on multiple physical or virtual servers.

## Before you begin {#before-start}

### Requirements {#requirements}

Review the [system requirements](../../../devops/concepts/system-requirements.md) and [cluster topology](../../../concepts/topology.md).

You must have SSH access to all servers. This is required to install artifacts and run the {{ ydb-short-name }} executable file.

The network configuration must allow TCP connections on the following ports (by default, they can be changed in settings):

* 22: SSH service
* 2135, 2136: gRPC for client-cluster interaction
* 19001, 19002: Interconnect for intra-cluster node interaction
* 8765, 8766: {{ ydb-short-name }} Embedded UI HTTP interface
* 9092, 9093: ports for Kafka API

When placing multiple dynamic nodes on the same server, separate ports are required for gRPC, Interconnect, HTTP interface, and Kafka API for each dynamic node on the server.

Ensure that the system clocks on all servers in the cluster are synchronized using `ntpd` or `chrony` tools. It is recommended to use a single time source for all cluster servers to ensure consistent handling of leap seconds.

If the Linux distribution used on the cluster servers employs `syslogd` for logging, you need to configure log file rotation using the `logrotate` tool or its equivalents. {{ ydb-short-name }} services can generate a significant amount of system logs, especially when the logging level is increased for diagnostic purposes, so it is important to enable system log file rotation to avoid `/var` file system overflow situations.

Select the servers and disks that will be used for data storage:

* Use the `block-4-2` fault tolerance scheme to deploy the cluster in a single availability zone (AZ), using at least 8 servers. This scheme can withstand the failure of 2 servers.
* Use the `mirror-3-dc` fault tolerance scheme to deploy the cluster in three availability zones (AZ), using at least 9 servers. This scheme can withstand the failure of 1 AZ and 1 server in another AZ. The number of servers used in each AZ must be the same.

{% note info %}

Run each static node (storage node) on a separate server. It is possible to co-locate static and dynamic nodes on the same server, as well as to place multiple dynamic nodes on one server if sufficient computing resources are available.

{% endnote %}

For more details on hardware requirements, see the [{#T}](../../../devops/concepts/system-requirements.md) section.

### Preparing TLS keys and certificates {#tls-certificates}

Traffic protection and authentication of {{ ydb-short-name }} server nodes are performed using the TLS protocol. Before installing the cluster, you need to plan the server composition, decide on the node naming scheme and specific names, and prepare TLS keys and certificates.

You can use existing certificates or generate new ones. The following TLS key and certificate files must be prepared in PEM format:

* `ca.crt` - Certificate Authority (CA) certificate that signs the other TLS certificates (same file on all cluster nodes).
* `node.key` - TLS private keys for each cluster node (a separate key for each cluster server).
* `node.crt` - TLS certificates for each cluster node (the certificate corresponding to the key).
* `web.pem` - concatenation of the node private key, node certificate, and Certificate Authority certificate for the monitoring HTTP interface (a separate file for each cluster server).

The required certificate generation parameters are determined by the organization's policy. Typically, certificates and keys for {{ ydb-short-name }} are generated with the following parameters:

* RSA keys with a length of 2048 or 4096 bits.
* SHA-256 certificate signing algorithm with RSA encryption.
* Node certificate validity period of at least 1 year.
* Certificate Authority certificate validity period of at least 3 years.

The certificate of the certification authority must be marked accordingly: the CA flag must be set, and the key usages "Digital Signature, Non Repudiation, Key Encipherment, Certificate Sign" must be enabled.

For node certificates, it is important that the actual host name (or host names) match the values specified in the "Subject Alternative Name" field. The certificates must have the key usages "Digital Signature, Key Encipherment" and the extended key usages "TLS Web Server Authentication, TLS Web Client Authentication" enabled. Node certificates must support both server and client authentication (the `extendedKeyUsage = serverAuth,clientAuth` option in OpenSSL settings).

For batch generation or renewal of {{ ydb-short-name }} cluster certificates using OpenSSL, you can use the [example script](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/tls_cert_gen/) hosted in the {{ ydb-short-name }} repository on GitHub. The script automatically generates the required key and certificate files for the entire set of cluster nodes in a single operation, simplifying installation preparation.

## Create a system user and group under which {{ ydb-short-name }} will run {#create-user}

On each server where {{ ydb-short-name }} will be started, run:

```bash
sudo groupadd ydb
sudo useradd ydb -g ydb
```

For the {{ ydb-short-name }} service to have access to block disks for operation, add the user under which the {{ ydb-short-name }} processes will run to the `disk` group:

```bash
sudo usermod -aG disk ydb
```

## Configure file descriptor limits {#file-descriptors}

For proper operation of {{ ydb-short-name }}, especially when using [spilling](../../../concepts/query_execution/spilling.md) in multi-node clusters, it is recommended to increase the limit on the number of simultaneously open file descriptors.

To change the file descriptor limit, add the following lines to the `/etc/security/limits.conf` file:

```bash
ydb soft nofile 10000
ydb hard nofile 10000
```

Where `ydb` is the name of the user under which `ydbd` is run.

After modifying the file, you need to reboot the system or log in again to apply the new limits.

{% note info %}

For more information about spilling configuration and its relation to file descriptors, see the ["Spilling configuration"](../../../reference/configuration/table_service_config.md#file-system-requirements) section.

{% endnote %}

## Install {{ ydb-short-name }} software on each server {#install-binaries}

1. Download and unpack the archive containing the `ydbd` executable and the libraries required for {{ ydb-short-name }} operation:

   ```bash
   mkdir ydbd-stable-linux-amd64
   curl -L <binaries_url> | tar -xz --strip-component=1 -C ydbd-stable-linux-amd64
   ```

   where `binaries_url` is a link to the archive of the required version from the [downloads](../../../downloads/index.md) page
2. Copy the executable and libraries to the appropriate directories:

   ```bash
   sudo cp -iR ydbd-stable-linux-amd64/bin /opt/ydb/
   sudo cp -iR ydbd-stable-linux-amd64/lib /opt/ydb/
   ```
3. Set the owner of the files and directories:

   ```bash
   sudo chown -R root:bin /opt/ydb
   ```

## Prepare and clean disks on each server {#prepare-disks}

{% include [_includes/storage-device-requirements.md](../../../_includes/storage-device-requirements.md) %}

You can get a list of block devices on the server with the `lsblk` command. Example output:

```txt
NAME   MAJ:MIN RM   SIZE RO TYPE MOUNTPOINTS
loop0    7:0    0  63.3M  1 loop /snap/core20/1822
...
vda    252:0    0    40G  0 disk
├─vda1 252:1    0     1M  0 part
└─vda2 252:2    0    40G  0 part /
vdb    252:16   0   186G  0 disk
└─vdb1 252:17   0   186G  0 part
```

Block device names depend on the operating system settings, either default or manually configured. Typically, device names consist of three parts:

- A fixed prefix or a prefix indicating the device type
- A sequential device identifier (can be a letter or a number)
- A sequential partition identifier on the device (usually a number)

1. Create partitions on the selected disks:

   {% note alert %}

   The following operation will delete all partitions on the specified disk! Make sure you have specified a disk that contains no other data!

   {% endnote %}

   ```bash
   DISK=/dev/nvme0n1
   sudo parted ${DISK} mklabel gpt -s
   sudo parted -a optimal ${DISK} mkpart primary 0% 100%
   sudo parted ${DISK} name 1 ydb_disk_ssd_01
   sudo partx --u ${DISK}
   ```

   Run the `ls -l /dev/disk/by-partlabel/` command to verify that a disk with the label `/dev/disk/by-partlabel/ydb_disk_ssd_01` has appeared in the system.

   If you plan to use more than one disk on each server, specify a unique label for each instead of `ydb_disk_ssd_01`. Disk labels must be unique within each server and are used in configuration files, as shown in the following instructions.

   To simplify subsequent configuration, it is convenient to use the same disk labels on cluster servers that have identical disk configurations.
2. Clean the disk using the command built into the `ydbd` executable:

   {% note warning %}

   After executing the command, the data on the disk will be erased.

   {% endnote %}

   ```bash
   sudo LD_LIBRARY_PATH=/opt/ydb/lib /opt/ydb/bin/ydbd admin bs disk obliterate /dev/disk/by-partlabel/ydb_disk_ssd_01
   ```

   Perform this operation for each disk that will be used to store {{ ydb-short-name }} data.

### Example of a full command for partitioning 3 disks

```bash
DISK=/dev/vdb
sudo parted ${DISK} mklabel gpt -s
sudo parted -a optimal ${DISK} mkpart primary 0% 100%
sudo parted ${DISK} name 1 ydb_disk_ssd_01
sudo partx --u ${DISK}
sleep 5
sudo LD_LIBRARY_PATH=/opt/ydb/lib /opt/ydb/bin/ydbd admin bs disk obliterate /dev/disk/by-partlabel/ydb_disk_ssd_01

DISK=/dev/vdc
sudo parted ${DISK} mklabel gpt -s
sudo parted -a optimal ${DISK} mkpart primary 0% 100%
sudo parted ${DISK} name 1 ydb_disk_ssd_02
sudo partx --u ${DISK}
sleep 5
sudo LD_LIBRARY_PATH=/opt/ydb/lib /opt/ydb/bin/ydbd admin bs disk obliterate /dev/disk/by-partlabel/ydb_disk_ssd_02

DISK=/dev/vdd
sudo parted ${DISK} mklabel gpt -s
sudo parted -a optimal ${DISK} mkpart primary 0% 100%
sudo parted ${DISK} name 1 ydb_disk_ssd_03
sudo partx --u ${DISK}
sleep 5
sudo LD_LIBRARY_PATH=/opt/ydb/lib /opt/ydb/bin/ydbd admin bs disk obliterate /dev/disk/by-partlabel/ydb_disk_ssd_03
```

### Verify disk preparation

To verify correct disk partitioning, run the command on each cluster server:

```bash
ls -al /dev/disk/by-partlabel/
```

The command output should show the disks you created and partitioned

```bash
lrwxrwxrwx 1 root root    10 Nov 26 12:54 ydb_disk_ssd_01 -> ../../vdb1
lrwxrwxrwx 1 root root    10 Nov 26 12:54 ydb_disk_ssd_02 -> ../../vdc1
lrwxrwxrwx 1 root root    10 Nov 26 12:54 ydb_disk_ssd_03 -> ../../vdd1
```

## Prepare configuration files {#config}

Prepare the {{ ydb-short-name }} configuration file:

```yaml
static_erasure: mirror-3-dc
host_configs:
- drive:
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_01
    type: SSD
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_02
    type: SSD
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_03
    type: SSD
  host_config_id: 1
hosts:
- host: static-node-1.ydb-cluster.com
  host_config_id: 1
  walle_location:
    body: 1
    data_center: 'zone-a'
    rack: '1'
- host: static-node-2.ydb-cluster.com
  host_config_id: 1
  walle_location:
    body: 2
    data_center: 'zone-b'
    rack: '2'
- host: static-node-3.ydb-cluster.com
  host_config_id: 1
  walle_location:
    body: 3
    data_center: 'zone-d'
    rack: '3'
domains_config:
  security_config:
    enforce_user_token_requirement: true
    default_users:
      - name: "root"
        password: ""
    default_access:
      - "+(F):root"
  domain:
  - name: Root
    storage_pool_types:
    - kind: ssd
      pool_config:
        box_id: 1
        erasure_species: mirror-3-dc
        kind: ssd
        geometry:
          realm_level_begin: 10
          realm_level_end: 20
          domain_level_begin: 10
          domain_level_end: 256
        pdisk_filter:
        - property:
          - type: SSD
        vdisk_kind: Default
  state_storage:
  - ring:
      node: [1, 2, 3]
      nto_select: 3
    ssid: 1
table_service_config:
  sql_version: 1
actor_system_config:
  executor:
  - name: System
    threads: 2
    type: BASIC
  - name: User
    threads: 3
    type: BASIC
  - name: Batch
    threads: 2
    type: BASIC
  - name: IO
    threads: 1
    time_per_mailbox_micro_secs: 100
    type: IO
  - name: IC
    spin_threshold: 10
    threads: 1
    time_per_mailbox_micro_secs: 100
    type: BASIC
  scheduler:
    progress_threshold: 10000
    resolution: 256
    spin_threshold: 0
blob_storage_config:
  service_set:
    groups:
    - erasure_species: mirror-3-dc
      rings:
      - fail_domains:
        - vdisk_locations:
          - node_id: static-node-1.ydb-cluster.com
            pdisk_category: SSD
            path: /dev/disk/by-partlabel/ydb_disk_ssd_01
        - vdisk_locations:
          - node_id: static-node-1.ydb-cluster.com
            pdisk_category: SSD
            path: /dev/disk/by-partlabel/ydb_disk_ssd_02
        - vdisk_locations:
          - node_id: static-node-1.ydb-cluster.com
            pdisk_category: SSD
            path: /dev/disk/by-partlabel/ydb_disk_ssd_03
      - fail_domains:
        - vdisk_locations:
          - node_id: static-node-2.ydb-cluster.com
            pdisk_category: SSD
            path: /dev/disk/by-partlabel/ydb_disk_ssd_01
        - vdisk_locations:
          - node_id: static-node-2.ydb-cluster.com
            pdisk_category: SSD
            path: /dev/disk/by-partlabel/ydb_disk_ssd_02
        - vdisk_locations:
          - node_id: static-node-2.ydb-cluster.com
            pdisk_category: SSD
            path: /dev/disk/by-partlabel/ydb_disk_ssd_03
      - fail_domains:
        - vdisk_locations:
          - node_id: static-node-3.ydb-cluster.com
            pdisk_category: SSD
            path: /dev/disk/by-partlabel/ydb_disk_ssd_01
        - vdisk_locations:
          - node_id: static-node-3.ydb-cluster.com
            pdisk_category: SSD
            path: /dev/disk/by-partlabel/ydb_disk_ssd_02
        - vdisk_locations:
          - node_id: static-node-3.ydb-cluster.com
            pdisk_category: SSD
            path: /dev/disk/by-partlabel/ydb_disk_ssd_03
channel_profile_config:
  profile:
  - channel:
    - erasure_species: mirror-3-dc
      pdisk_category: 0
      storage_pool_kind: ssd
    - erasure_species: mirror-3-dc
      pdisk_category: 0
      storage_pool_kind: ssd
    - erasure_species: mirror-3-dc
      pdisk_category: 0
      storage_pool_kind: ssd
    profile_id: 0
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
client_certificate_authorization:
  request_client_certificate: true
  client_certificate_definitions:
    - member_groups: ["registerNode@cert"]
      subject_terms:
      - short_name: "O"
        values: ["YDB"]
```

To speed up and simplify the initial deployment of {{ ydb-short-name }}, the configuration file already contains most of the settings for installing the cluster. Just replace the default FQDN hosts with the actual ones in the `hosts` and `blob_storage_config` sections.

* Section `hosts`:

  ```yaml
  ...
  hosts:
    - host: static-node-1.ydb-cluster.com #FQDN VM
      host_config_id: 1
      walle_location:
        body: 1
        data_center: 'zone-a'
        rack: '1'
  ...
  ```
* Section `blob_storage_config`:

  ```yaml
  ...
  - fail_domains:
    - vdisk_locations:
      - node_id: static-node-1.ydb-cluster.com #FQDN VM
        pdisk_category: SSD
        path: /dev/disk/by-partlabel/ydb_disk_1
  ...
  ```

The remaining sections and settings of the configuration file remain unchanged.

Save the YDB configuration file as `/opt/ydb/cfg/config.yaml` on each cluster server.

For more information on creating the configuration file, see the [{#T}](../../../reference/configuration/index.md) section.

## Copy TLS keys and certificates to each server {#tls-copy-cert}

The prepared TLS keys and certificates must be copied to a secure directory on each cluster node {{ ydb-short-name }}. Below is an example of commands for creating a secure directory and copying the key and certificate files.

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

{% list tabs group=manual-systemd %}

- Manually

  Start the {{ ydb-short-name }} storage service on each static cluster node:

  ```bash
  sudo su - ydb
  cd /opt/ydb
  export LD_LIBRARY_PATH=/opt/ydb/lib
  /opt/ydb/bin/ydbd server --log-level 3 --syslog --tcp --config-dir /opt/ydb/cfg \
      --grpcs-port 2135 --ic-port 19001 --mon-port 8765 --mon-cert /opt/ydb/certs/web.pem --node static
  ```
- Using systemd

  On each server that will host a static cluster node, create a systemd configuration file `/etc/systemd/system/ydbd-storage.service` according to the sample below. You can also [download the sample from the repository](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/systemd_services/ydbd-storage.service).

  ```ini
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
      --config-dir /opt/ydb/cfg \
      --grpcs-port 2135 --ic-port 19001 --mon-port 8765 \
      --mon-cert /opt/ydb/certs/web.pem --node static
  LimitNOFILE=65536
  LimitCORE=0
  LimitMEMLOCK=3221225472

  [Install]
  WantedBy=multi-user.target
  ```

  Start the service on each static node {{ ydb-short-name }}:

  ```bash
  sudo systemctl start ydbd-storage
  ```

{% endlist %}

After starting the static nodes, verify their operation via the {{ ydb-short-name }} built-in web interface (Embedded UI):

1. Open the address `https://<node.ydb.tech>:8765` in a browser, where `<node.ydb.tech>` is the FQDN of the server running any static node.
2. Go to the **Nodes** tab.
3. Make sure all 3 static nodes are displayed in the list.

![Manual installation, running static nodes](../_assets/manual_installation_1.png)

## Initialize the cluster {#initialize-cluster}

The cluster initialization operation configures the set of static nodes listed in the cluster configuration file for {{ ydb-short-name }} data storage.

To initialize the cluster, you will need the certificate authority file `ca.crt`, whose path must be specified when running the relevant commands. Before running the commands, copy the `ca.crt` file to the server where the commands will be executed.

On one of the storage servers in the cluster, run the commands:

First, obtain an authorization token for registering requests. To do this, run the command below.

```bash
/opt/ydb/bin/ydb --ca-file ca.crt -e grpcs://`hostname -f`:2135 -d /Root --user root --no-password auth get-token -f > auth_token
```

Initialize the cluster using the obtained token

```bash
export LD_LIBRARY_PATH=/opt/ydb/lib
/opt/ydb/bin/ydbd --ca-file ca.crt -s grpcs://`hostname -f`:2135 -f auth_token \
    admin blobstorage config init --yaml-file  /opt/ydb/cfg/config.yaml
echo $?
```

If the cluster initialization is successful, the exit code of the cluster initialization command should be zero.

## Create a database {#create-db}

To work with row-based or column-based tables, you need to create at least one database and start the process or processes that serve this database (dynamic nodes).

To run the administrative command for creating a database, you will need the certificate authority file `ca.crt`, similar to the procedure described above for cluster initialization.

When creating a database, the initial number of storage groups used is set, which determines the available I/O throughput and maximum storage capacity. The number of storage groups can be increased after the database is created if necessary.

On one of the storage servers in the cluster, run the commands:

```bash
export LD_LIBRARY_PATH=/opt/ydb/lib
/opt/ydb/bin/ydbd --ca-file ca.crt -s grpcs://`hostname -f`:2135 -f auth_token \
    admin database /Root/testdb create ssd:8
echo $?
```

If the database is created successfully, the exit code of the command should be zero.

The command example above uses the following parameters:

* `/Root` — the name of the root domain automatically generated during cluster initialization.
* `testdb` — the name of the database being created.
* `ssd:8` — sets the storage pool for the database and the number of groups in it. The pool name (`ssd`) must match the disk type specified in the cluster configuration (for example, in `default_disk_type`) and is case-insensitive. The number after the colon is the number of allocated storage groups.

## Start dynamic nodes {#start-dynnode}

{% list tabs group=manual-systemd %}

- Manually

  Start the dynamic node {{ ydb-short-name }} for the `/Root/testdb` database:

  ```bash
  sudo su - ydb
  cd /opt/ydb
  export LD_LIBRARY_PATH=/opt/ydb/lib
  /opt/ydb/bin/ydbd server --grpcs-port 2136 --grpc-ca /opt/ydb/certs/ca.crt \
      --ic-port 19002 --ca /opt/ydb/certs/ca.crt \
      --mon-port 8766 --mon-cert /opt/ydb/certs/web.pem \
      --kafka-port 9093 \
      --config-dir /opt/ydb/cfg \
      --tenant /Root/testdb \
      --grpc-cert /opt/ydb/certs/node.crt \
      --grpc-key /opt/ydb/certs/node.key \
      --node-broker grpcs://<ydb-static-node1>:2135 \
      --node-broker grpcs://<ydb-static-node2>:2135 \
      --node-broker grpcs://<ydb-static-node3>:2135
  ```

  In the command example above, `<ydb-static-node1>`, `<ydb-static-node2>`, `<ydb-static-node3>` are the FQDNs of any three servers running static cluster nodes.
- Using systemd

  Create a systemd configuration file `/etc/systemd/system/ydbd-testdb.service` using the sample below. You can also [download the sample file from the repository](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/systemd_services/ydbd-testdb.service).

  ```ini
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
      --kafka-port 9093 \
      --config-dir /opt/ydb/cfg \
      --tenant /Root/testdb \
      --grpc-cert /opt/ydb/certs/node.crt \
      --grpc-key /opt/ydb/certs/node.key \
      --node-broker grpcs://<ydb-static-node1>:2135 \
      --node-broker grpcs://<ydb-static-node2>:2135 \
      --node-broker grpcs://<ydb-static-node3>:2135
  LimitNOFILE=65536
  LimitCORE=0
  LimitMEMLOCK=32212254720

  [Install]
  WantedBy=multi-user.target
  ```

  In the example command above, `<ydb-static-node1>`, `<ydb-static-node2>`, `<ydb-static-node3>` - FQDNs of any three servers on which the static cluster nodes are running.

  Run the dynamic node {{ ydb-short-name }} for the database `/Root/testdb`:

  ```bash
  sudo systemctl start ydbd-testdb
  ```

{% endlist %}

Run additional dynamic nodes on other servers to scale and ensure database fault tolerance.

## Setting up accounts {#security-setup}

1. Set a password for the `root` account using the previously obtained token:

   ```bash
   ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --token-file auth_token \
       yql -s 'ALTER USER root PASSWORD "passw0rd"'
   ```

   Instead of the `passw0rd` value, substitute the required password. Save the password to a separate file. Subsequent commands on behalf of user `root` will be executed using the password passed via the `--password-file <path_to_user_password>` flag. You can also save the password in the connection profile, as described in the [{{ ydb-short-name }} CLI documentation](../../../reference/ydb-cli/profile/index.md).
2. Create additional accounts:

   ```bash
   ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root --password-file <path_to_root_pass_file> \
       yql -s 'CREATE USER user1 PASSWORD "passw0rd"'
   ```
3. Set account permissions by including them in built-in groups:

   ```bash
   ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root --password-file <path_to_root_pass_file> \
       yql -s 'ALTER GROUP `ADMINS` ADD USER user1'
   ```

In the command examples listed above, `<node.ydb.tech>` is the FQDN of the server where any dynamic node serving the `/Root/testdb` database is running. When connecting via SSH to a dynamic node {{ ydb-short-name }}, it is convenient to use the `grpcs://$(hostname -f):2136` construct to obtain the FQDN.

When executing commands to create user accounts and assign groups, the {{ ydb-short-name }} CLI client will prompt you to enter the password of user `root`. To avoid repeatedly entering the password, you can create a connection profile as described in the [{{ ydb-short-name }} CLI documentation](../../../reference/ydb-cli/profile/index.md).

## Test the created database {#try-first-db}

1. Install the {{ ydb-short-name }} CLI as described in the [documentation](../../../reference/ydb-cli/install.md).
2. Create a test row-based (`test_row_table`) or column-based table (`test_column_table`):

{% list tabs %}

- Creating a row table

  ```bash
  ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root \
      yql -s 'CREATE TABLE `testdir/test_row_table` (id Uint64, title Utf8, PRIMARY KEY (id));'
  ```

- Creating a columnar table

  ```bash
  ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root \
      yql -s 'CREATE TABLE `testdir/test_column_table` (id Uint64 NOT NULL, title Utf8, PRIMARY KEY (id)) WITH (STORE = COLUMN);'
  ```

{% endlist %}

Where `<node.ydb.tech>` is the FQDN of the server on which the dynamic node serving the database `/Root/testdb` is running.

## Checking access to the built-in web interface

To check access to the built-in web interface {{ ydb-short-name }}, simply open a page with the address `https://<node.ydb.tech>:8765` in a web browser, where `<node.ydb.tech>` is the FQDN of the server on which any static node {{ ydb-short-name }} is running.

The web browser must have trust in the certificate authority that issued the certificates for the {{ ydb-short-name }} cluster, otherwise a warning about using an untrusted certificate will be displayed.

If authentication is enabled in the cluster, a login and password prompt should appear in the web browser. After entering valid authentication data, the start page of the built-in web interface should be displayed. A description of the available functions and user interface is provided in the [{#T}](../../../reference/embedded-ui/index.md) section.

{% note info %}

Typically, to provide access to the built-in web interface of {{ ydb-short-name }}, a fault-tolerant HTTP load balancer based on `haproxy`, `nginx`, or similar software is configured. Details of configuring the HTTP load balancer are beyond the scope of the standard {{ ydb-short-name }} installation instructions.

{% endnote %}

## Features of installing {{ ydb-short-name }} in unsecured mode

{% note warning %}

We do not recommend using the unsecured mode of {{ ydb-short-name }} either during operation or during application development.

{% endnote %}

The installation procedure described above involves deployment of {{ ydb-short-name }} in the standard secure mode.

The unsecured mode of operation of {{ ydb-short-name }} is intended for solving test tasks, primarily related to the development and testing of {{ ydb-short-name }} software. In unsecured mode:

* traffic between cluster nodes, as well as between applications and the cluster, uses unencrypted connections
* user authentication is not used (enabling authentication without traffic encryption is pointless, since login and password would be transmitted over the network in plain text in such a configuration).

Installation of {{ ydb-short-name }} for operation in unsecured mode is performed in the order described above, with the following exceptions:

1. When preparing for installation, you do not need to generate TLS certificates and keys, and you do not need to copy them to the cluster nodes.
2. Sections `security_config`, `interconnect_config`, and `grpc_config` are excluded from the configuration files of cluster nodes.
3. A simplified version of the commands for starting static and dynamic cluster nodes is used: options with certificate and key file names are omitted, the `grpc` protocol is used instead of `grpcs` when specifying connection endpoints.
4. The unnecessary step of obtaining an authentication token before initializing the cluster and creating a database is skipped in unsecured mode.
5. The cluster initialization command is executed in the following form:

   ```bash
   export LD_LIBRARY_PATH=/opt/ydb/lib
   ydb admin cluster bootstrap --uuid <строка>
   echo $?
   ```
6. The database creation command is executed in the following form:

   ```bash
   export LD_LIBRARY_PATH=/opt/ydb/lib
   /opt/ydb/bin/ydbd admin database /Root/testdb create ssd:1
   ```
7. When accessing the database from the {{ ydb-short-name }} CLI and applications, the grpc protocol is used instead of grpcs, and authentication is not used.
