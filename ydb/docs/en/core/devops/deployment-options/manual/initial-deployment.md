# Deploying a {{ ydb-short-name }} cluster manually

{% note warning %}

This guide is intended only for deploying clusters with [Configuration V1](../../configuration-management/configuration-v1/index.md). Deploying clusters with [Configuration V2](../../configuration-management/configuration-v2/index.md) is currently under development.

{% endnote %}

This document describes how to deploy a multitenant {{ ydb-short-name }} cluster on multiple physical or virtual servers.

## Before you start {#before-start}

### Requirements {#requirements}

Review the [system requirements](../../../devops/concepts/system-requirements.md) and [cluster topology](../../../concepts/topology.md).

You must have SSH access to all servers. This is required to install the artifacts and run the {{ ydb-short-name }} executable file.

The network configuration must allow TCP connections on the following ports (default, can be changed by settings):

* 22: SSH service;
* 2135, 2136: gRPC for client-cluster interaction;
* 19001, 19002: Interconnect for intra-cluster node interaction;
* 8765, 8766: HTTP interface of {{ ydb-short-name }} Embedded UI;
* 9092, 9093: ports for Kafka API operations.

If you place multiple dynamic nodes on one server, you will need separate ports for gRPC, Interconnect, the HTTP interface, and Kafka API for each dynamic node on the server.

Make sure that the system clocks on all servers in the cluster are synchronized using `ntpd` or `chrony` tools. It is advisable to use a single time source for all cluster servers to ensure consistent handling of leap seconds.

If the Linux type used on the cluster servers uses `syslogd` for logging, you must configure log rotation using `logrotate` or its analogs. {{ ydb-short-name }} services can generate a significant amount of system logs, especially when the logging level is increased for diagnostic purposes, so it is important to enable log rotation to prevent `/var` filesystem overflow.

Select the servers and disks that will be used for data storage:

* Use the `block-4-2` fault tolerance scheme to deploy a cluster in a single availability zone (AZ), using at least 8 servers. This scheme allows you to withstand the failure of 2 servers.
* Use the `mirror-3-dc` fault tolerance scheme to deploy a cluster across three availability zones (AZs), using at least 9 servers. This scheme allows you to withstand the failure of 1 AZ and 1 server in another AZ. The number of servers in each AZ must be the same.

{% note info %}

Run each static node (data storage node) on a separate server. It is possible to combine static and dynamic nodes on the same server, as well as place multiple dynamic nodes on one server if there are sufficient computing resources.

{% endnote %}

For more details on hardware requirements, see [{#T}](../../../devops/concepts/system-requirements.md).

### Preparing TLS keys and certificates {#tls-certificates}

{{ ydb-short-name }} server node traffic protection and authentication are implemented using the TLS protocol. Before installing the cluster, you need to plan the server composition, decide on the node naming scheme and specific names, and prepare TLS keys and certificates.

You can use existing or generate new certificates. The following TLS key and certificate files must be prepared in PEM format:

* `ca.crt` — the certificate of the Certification Authority (CA) that signs the other TLS certificates (the same files on all cluster nodes);
* `node.key` — TLS private keys for each cluster node (a separate key for each cluster server);
* `node.crt` — TLS certificates for each cluster node (matching the key certificate);
* `web.pem` — concatenation of the node's private key, node certificate, and CA certificate for the monitoring HTTP interface (a separate file for each cluster server).

The required certificate generation parameters are determined by the organization's policy. Typically, {{ ydb-short-name }} certificates and keys are generated with the following parameters:

* RSA keys with a length of 2048 or 4096 bits;
* SHA-256 signature algorithm with RSA encryption;
* node certificates valid for at least 1 year;
* CA certificate valid for at least 3 years.

The CA certificate must be marked accordingly: the CA flag must be set, and the following uses must be enabled: "Digital Signature, Non Repudiation, Key Encipherment, Certificate Sign".

For node certificates, it is important that the actual hostname (or hostnames) matches the values specified in the "Subject Alternative Name" field. The certificates must include the uses "Digital Signature, Key Encipherment" and the extended uses "TLS Web Server Authentication, TLS Web Client Authentication". It is necessary that node certificates support both server and client authentication (the `extendedKeyUsage = serverAuth,clientAuth` option in OpenSSL settings).

To batch generate or update {{ ydb-short-name }} cluster certificates using OpenSSL, you can use the [script example](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/tls_cert_gen/) from the {{ ydb-short-name }} GitHub repository. The script allows you to automatically generate the necessary key and certificate files for the entire set of cluster nodes in a single operation, simplifying preparation for installation.

## Create a system user and group on behalf of which {{ ydb-short-name }} will run {#create-user}

On each server where {{ ydb-short-name }} will be running, execute:

```bash
sudo groupadd ydb
sudo useradd ydb -g ydb
```

To allow the {{ ydb-short-name }} service to access block disks, add the user under which {{ ydb-short-name }} processes will run to the `disk` group:

```bash
sudo usermod -aG disk ydb
```

## Configure file descriptor limits {#file-descriptors}

For {{ ydb-short-name }} to work correctly, especially when using [spilling](../../../concepts/query_execution/spilling.md) in multi-node clusters, it is recommended to increase the limit on the number of simultaneously open file descriptors.

To change the file descriptor limit, add the following lines to the `/etc/security/limits.conf` file:

```bash
ydb soft nofile 10000
ydb hard nofile 10000
```

Where `ydb` is the username under which `ydbd` runs.

After modifying the file, you must reboot the system or log in again for the new limits to take effect.

{% note info %}

For more information about spilling configuration and its relation to file descriptors, see the [«Spilling Configuration»](../../../reference/configuration/table_service_config.md#file-system-requirements) section.

{% endnote %}

## Install {{ ydb-short-name }} software on each server {#install-binaries}

1. Download and unpack the archive with the `ydbd` executable file and the libraries required for {{ ydb-short-name }} to work:

    ```bash
    mkdir ydbd-stable-linux-amd64
    curl -L <binaries_url> | tar -xz --strip-component=1 -C ydbd-stable-linux-amd64
    ```
where `binaries_url` is the link to the archive of the desired version from the [downloads](../../../downloads/index.md) page

1. Copy the executable file and libraries to the appropriate directories:

    ```bash
    sudo cp -iR ydbd-stable-linux-amd64/bin /opt/ydb/
    sudo cp -iR ydbd-stable-linux-amd64/lib /opt/ydb/
    ```

1. Set the owner of the files and directories:

    ```bash
    sudo chown -R root:bin /opt/ydb
    ```

## Prepare and clean disks on each server {#prepare-disks}

{% include [_includes/storage-device-requirements.md](../../../_includes/storage-device-requirements.md) %}

You can get a list of block devices on the server using the `lsblk` command. Example output:

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

The names of block devices depend on the operating system settings, either set by the base image or configured manually. Typically, device names consist of three parts:

* A fixed prefix or a prefix indicating the device type;
* A sequential device identifier (can be a letter or a number);
* A sequential partition identifier on the device (usually a number).

1. Create partitions on the selected disks:

    {% note alert %}

    The following operation will delete all partitions on the specified disk! Make sure you have specified a disk that does not contain other data!

    {% endnote %}

    ```bash
    DISK=/dev/nvme0n1
    sudo parted ${DISK} mklabel gpt -s
    sudo parted -a optimal ${DISK} mkpart primary 0% 100%
    sudo parted ${DISK} name 1 ydb_disk_ssd_01
    sudo partx --u ${DISK}
    ```

    Run the `ls -l /dev/disk/by-partlabel/` command to verify that the system has a disk with the label `/dev/disk/by-partlabel/ydb_disk_ssd_01`.

    If you plan to use more than one disk on each server, specify a unique label for each disk instead of `ydb_disk_ssd_01`. Disk labels must be unique within each server and are used in configuration files as shown in the following instructions.

    To simplify subsequent configuration, it is convenient to use the same disk labels on cluster servers with identical disk configurations.

2. Clean the disk using the command built into the `ydbd` executable file:

    {% note warning %}

    Running this command will erase data on the disk.

    {% endnote %}

    ```bash
    sudo LD_LIBRARY_PATH=/opt/ydb/lib /opt/ydb/bin/ydbd admin bs disk obliterate /dev/disk/by-partlabel/ydb_disk_ssd_01
    ```

    Perform this operation for each disk that will be used to store {{ ydb-short-name }} data.

### Example of a complete command for partitioning 3 disks

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

To check the correct disk partitioning, run the command on each cluster server:

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

To speed up and simplify the initial deployment of {{ ydb-short-name }}, the configuration file already contains most of the settings for cluster installation. It is enough to replace the standard FQDN hosts with actual ones in the `hosts` and `blob_storage_config` sections.

* `hosts` section:

  ```yaml
  ...
  hosts:
    - host: static-node-1.ydb-cluster.com #VM FQDN
      host_config_id: 1
      walle_location:
        body: 1
        data_center: 'zone-a'
        rack: '1'
  ...
  ```

* `blob_storage_config` section:

  ```yaml
  ...
  - fail_domains:
    - vdisk_locations:
      - node_id: static-node-1.ydb-cluster.com #VM FQDN
        pdisk_category: SSD
        path: /dev/disk/by-partlabel/ydb_disk_1
  ...
  ```

The rest of the configuration file sections and settings remain unchanged.

Save the {{ ydb-short-name }} configuration file as `/opt/ydb/cfg/config.yaml` on each cluster server.

For more detailed information on creating a configuration file, see [{#T}](../../../reference/configuration/index.md).

## Copy TLS keys and certificates to each server {#tls-copy-cert}

The prepared TLS keys and certificates must be copied to a secure directory on each {{ ydb-short-name }} cluster node. Below is an example of commands to create a secure directory and copy the key and certificate files.

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

  Run the {{ ydb-short-name }} data storage service on each static cluster node:
  ```bash
  sudo su - ydb
  cd /opt/ydb
  export LD_LIBRARY_PATH=/opt/ydb/lib
  /opt/ydb/bin/ydbd server --log-level 3 --syslog --tcp --config-dir /opt/ydb/cfg \
      --grpcs-port 2135 --ic-port 19001 --mon-port 8765 --mon-cert /opt/ydb/certs/web.pem --node static
  ```
- Using systemd

Create a systemd configuration file `/etc/systemd/system/ydbd-storage.service` on each server where a static cluster node will be located, using the example below. You can also [download the file from the repository](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/systemd_services/ydbd-storage.service).
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
Run the service on each static {{ ydb-short-name }} node:
  ```bash
  sudo systemctl start ydbd-storage
  ```
{% endlist %}

After starting the static nodes, check their functionality through the {{ ydb-short-name }} built-in web interface (Embedded UI):

1. Open the address `https://<node.ydb.tech>:8765` in your browser, where `<node.ydb.tech>` is the FQDN of the server running any static node;
2. Go to the **Nodes** tab;
3. Make sure that all 3 static nodes are displayed in the list.

![Manual installation, running static nodes](../_assets/manual_installation_1.png)

## Initialize the cluster {#initialize-cluster}

The cluster initialization operation configures the set of static nodes listed in the cluster configuration file to store {{ ydb-short-name }} data.

To initialize the cluster, you need a registration authority certificate file `ca.crt`, the path to which must be specified when executing the corresponding commands. Before running the commands, copy the `ca.crt` file to the server where the commands will be executed.

On one of the storage servers in the cluster, run the following commands:

First, get an authorization token for registering requests. To do this, run the command below.

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

If the cluster initialization is successful, the command completion code displayed on the screen should be zero.

## Create a database {#create-db}

To work with string or columnar tables, you need to create at least one database and start the process or processes serving this database (dynamic nodes).

To execute the administrative command to create a database, you need a registration authority certificate file `ca.crt`, following the procedure described above for cluster initialization.

When creating a database, the initial number of storage groups is set, which determines the available I/O throughput and maximum storage capacity. The number of storage groups can be increased after the database is created if necessary.

On one of the storage servers in the cluster, run the following commands:

```bash
export LD_LIBRARY_PATH=/opt/ydb/lib
/opt/ydb/bin/ydbd --ca-file ca.crt -s grpcs://`hostname -f`:2135 -f auth_token \
    admin database /Root/testdb create ssd:8
echo $?
```

If the database is created successfully, the command completion code displayed on the screen should be zero.

The following parameters are used in the command example above:

* `/Root` — the name of the root domain generated automatically during cluster initialization;
* `testdb` — the name of the database being created;
* `ssd:8` — specifies the storage pool for the database and the number of groups in it. The pool name (`ssd`) must match the disk type specified in the cluster configuration (for example, in `default_disk_type`) and is case-insensitive. The number after the colon is the number of storage groups allocated.

## Start dynamic nodes {#start-dynnode}

{% list tabs group=manual-systemd %}

- Manually

  Start a dynamic node of {{ ydb-short-name }} for the `/Root/testdb` database:
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
In the example command above, `<ydb-static-node1>`, `<ydb-static-node2>`, `<ydb-static-node3>` are the FQDNs of any three servers running static cluster nodes.

- Using systemd

Create a systemd config file `/etc/systemd/system/ydbd-testdb.service` using the example below. You can also [download the file from the repository](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/systemd_services/ydbd-testdb.service).
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
In the example command above, `<ydb-static-node1>`, `<ydb-static-node2>`, `<ydb-static-node3>` are the FQDNs of any three servers running static cluster nodes.

Start a dynamic {{ ydb-short-name }} node for the `/Root/testdb` database:
  ```bash
  sudo systemctl start ydbd-testdb
  ```
{% endlist %}

Launch additional dynamic nodes on other servers to scale and ensure database fault tolerance.

## Setting up accounts {#security-setup}

1. Set a password for the `root` account using the previously obtained token:

    ```bash
    ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --token-file auth_token \
        yql -s 'ALTER USER root PASSWORD "passw0rd"'
    ```

    Replace the `passw0rd` value with the desired password. Save the password to a separate file. Subsequent commands on behalf of the `root` user will be executed using the password passed with the `--password-file <path_to_user_password>` key. You can also save the password in the connection profile as described in the {{ ydb-short-name }} [CLI documentation](../../../reference/ydb-cli/profile/index.md).

1. Create additional accounts:

    ```bash
    ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root --password-file <path_to_root_pass_file> \
        yql -s 'CREATE USER user1 PASSWORD "passw0rd"'
    ```

1. Set account permissions by adding them to built-in groups:

    ```bash
    ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root --password-file <path_to_root_pass_file> \
        yql -s 'ALTER GROUP `ADMINS` ADD USER user1'
    ```

In the command examples listed above, `<node.ydb.tech>` is the FQDN of the server running any dynamic node serving the `/Root/testdb` database. When connecting to a {{ ydb-short-name }} dynamic node via SSH, it is convenient to use the `grpcs://$(hostname -f):2136` construct to obtain the FQDN.

When executing commands to create accounts and assign groups, the {{ ydb-short-name }} CLI client will prompt for the `root` user password. You can avoid entering the password multiple times by creating a connection profile as described in the {{ ydb-short-name }} CLI documentation (../../../reference/ydb-cli/profile/index.md).

## Test the created database {#try-first-db}

1. Install {{ ydb-short-name }} [CLI as described in the documentation](../../../reference/ydb-cli/install.md).

1. Create a test row (`test_row_table`) or column (`test_column_table`) table:

{% list tabs %}

- Creating a string table
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

Where `<node.ydb.tech>` is the FQDN of the server running the dynamic node that serves the `/Root/testdb` database.

## Checking access to the built-in web interface

To check access to the built-in {{ ydb-short-name }} web interface, open the page `https://<node.ydb.tech>:8765` in your web browser, where `<node.ydb.tech>` is the FQDN of the server running any static {{ ydb-short-name }} node.

Your web browser must trust the certificate authority that issued the certificates for the {{ ydb-short-name }} cluster, otherwise you will see a warning about an untrusted certificate.

If authentication is enabled in the cluster, your web browser will prompt for a username and password. After entering the correct authentication credentials, the initial page of the built-in web interface should appear. A description of the available functions and user interface is provided in the section [{#T}](../../../reference/embedded-ui/index.md).

{% note info %}

Typically, to provide access to the built-in {{ ydb-short-name }} web interface, a fault-tolerant HTTP load balancer based on software like `haproxy`, `nginx`, or similar is configured. The details of configuring an HTTP load balancer are beyond the scope of the standard {{ ydb-short-name }} installation instructions.

{% endnote %}

## Features of installing {{ ydb-short-name }} in insecure mode

{% note warning %}

We do not recommend using the insecure mode of {{ ydb-short-name }} either in production or during application development.

{% endnote %}

The installation procedure described above deploys {{ ydb-short-name }} in the standard secure mode.

The insecure mode of {{ ydb-short-name }} is intended for solving test tasks primarily related to the development and testing of {{ ydb-short-name }} software. In insecure mode:

* traffic between cluster nodes and between applications and the cluster uses unencrypted connections;
* user authentication is not used (enabling authentication without traffic encryption makes no sense, as the username and password would be transmitted over the network in plain text in such a configuration).

Installing {{ ydb-short-name }} to operate in insecure mode follows the procedure described above, with the following exceptions:

1. When preparing for installation, there is no need to generate TLS certificates and keys, and no need to copy certificates and keys to the cluster nodes.
1. The `security_config`, `interconnect_config`, and `grpc_config` sections are removed from the cluster node configuration files.
1. A simplified version of the commands for starting static and dynamic cluster nodes is used: options with certificate and key file names are excluded, and the `grpc` protocol is used instead of `grpcs` when specifying connection points.
1. The step of obtaining an authentication token before initializing the cluster and creating a database, which is unnecessary in insecure mode, is skipped.
1. The cluster initialization command is executed in the following form:

    ```bash
    export LD_LIBRARY_PATH=/opt/ydb/lib
    ydb admin cluster bootstrap --uuid <string>
    echo $?
    ```

1. The database creation command is executed in the following form:

    ```bash
    export LD_LIBRARY_PATH=/opt/ydb/lib
    /opt/ydb/bin/ydbd admin database /Root/testdb create ssd:1
    ```

1. When accessing the database from {{ ydb-short-name }} CLI and applications, the `grpc` protocol is used instead of `grpcs`, and authentication is not used.
