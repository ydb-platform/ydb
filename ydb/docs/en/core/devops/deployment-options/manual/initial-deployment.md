# Deploying {{ ydb-short-name }} Cluster Manually

<!-- markdownlint-disable blanks-around-fences -->
{% note warning %}

This guide is only for deploying clusters with [V1 configuration](../../configuration-management/configuration-v1/index.md). Deploying clusters with [V2 configuration](../../configuration-management/configuration-v2/index.md) is currently under development.

{% endnote %}

This document describes how to deploy a multi-tenant {{ ydb-short-name }} cluster on multiple bare-metal or virtual servers.

## Getting Started {#before-start}

### Prerequisites {#requirements}

Review the [system requirements](../../../devops/concepts/system-requirements.md) and the [cluster topology](../../../concepts/topology.md).

Make sure you have SSH access to all servers. This is required to install artifacts and run the {{ ydb-short-name }} executable.

The network configuration must allow TCP connections on the following ports (these are defaults, but you can change them by settings):

* 22: SSH service;
* 2135, 2136: GRPC for client-cluster interaction;
* 19001, 19002: Interconnect for intra-cluster node interaction;
* 8765, 8766: HTTP interface of {{ ydb-short-name }} Embedded UI;
* 9092, 9093: ports for Kafka API;

Distinct ports are necessary for gRPC, Interconnect, HTTP interface and Kafka API of each dynamic node within the server when hosting multiple dynamic nodes on a single server.

Make sure that the system clocks running on all the cluster's servers are synced by `ntpd` or `chrony`. It is advisable to use the same time source for all servers in the cluster to ensure consistent leap seconds processing.

If the Linux flavor run on the cluster servers uses `syslogd` for logging, set up log file rotation using`logrotate` or similar tools. {{ ydb-short-name }} services can generate substantial amounts of system logs, particularly when you elevate the logging level for diagnostic purposes. That's why it's important to enable system log file rotation to prevent the `/var` file system overflow.

Select the servers and disks to be used for storing data:

* Use the `block-4-2` fault tolerance model for cluster deployment in one availability zone (AZ). Use at least eight servers to survive the loss of two servers.
* Use the `mirror-3-dc` fault tolerance model for cluster deployment in three availability zones (AZ). To survive the loss of one AZ and one server in another AZ, use at least nine servers. Make sure that the number of servers running in each AZ is the same.

{% note info %}

Run each static node (data node) on a separate server. Both static and dynamic nodes can run together on the same server. A server can also run multiple dynamic nodes if it has enough computing power.

{% endnote %}

For more information about hardware requirements, see [{#T}](../../../devops/concepts/system-requirements.md).

### Preparing TLS Keys and Certificates {#tls-certificates}

The TLS protocol provides traffic protection and authentication for {{ ydb-short-name }} server nodes. Before you install your cluster, determine which servers it will host, establish the node naming convention, come up with node names, and prepare your TLS keys and certificates.

You can use existing certificates or generate new ones. Prepare the following files with TLS keys and certificates in the PEM format:

* `ca.crt`: CA-issued certificate used to sign the other TLS certificates (these files are the same on all the cluster nodes).
* `node.key`: Secret TLS keys for each cluster node (one key per cluster server).
* `node.crt`: TLS certificates for each cluster node (each certificate corresponds to a key).
* `web.pem`: Concatenation of the node secret key, node certificate, and the CA certificate needed for the monitoring HTTP interface (a separate file is used for each server in the cluster).

The required parameters for certificate generation are determined by the organization's policy. The following parameters are commonly used for generating certificates and keys for {{ ydb-short-name }}:

* 2048-bit or 4096-bit RSA keys
* Certificate signing algorithm: SHA-256 with RSA encryption
* Validity period of node certificates: at least 1 year
* CA certificate validity period: at least 3 years.

Make sure that the CA certificate is appropriately labeled, with the CA property enabled along with the "Digital Signature, Non Repudiation, Key Encipherment, Certificate Sign" usage types.

For node certificates, it's key that the actual host name (or names) match the values in the "Subject Alternative Name" field. Enable both the regular usage types ("Digital Signature, Key Encipherment") and advanced usage types ("TLS Web Server Authentication, TLS Web Client Authentication") for the certificates. Node certificates must support both server authentication and client authentication (the `extendedKeyUsage = serverAuth,clientAuth` option in the OpenSSL settings).

For batch generation or update of {{ ydb-short-name }} cluster certificates by OpenSSL, you can use the [sample script](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/tls_cert_gen/) from the {{ ydb-short-name }} GitHub repository. Using the script, you can streamline preparation for installation, automatically generating all the key files and certificate files for all your cluster nodes in a single step.
## Create a System User and a Group to Run {{ ydb-short-name }} {#create-user}

On each server that will be running {{ ydb-short-name }}, execute the command below:

```bash
sudo groupadd ydb
sudo useradd ydb -g ydb
```

To ensure that {{ ydb-short-name }} can access block disks, add the user that will run {{ ydb-short-name }} processes, to the `disk` group:

```bash
sudo usermod -aG disk ydb
```

## Configure File Descriptor Limits {#file-descriptors}

For proper operation of {{ ydb-short-name }}, especially when using [spilling](../../../concepts/query_execution/spilling.md) in multi-node clusters, it is recommended to increase the limit of simultaneously open file descriptors.

To change the file descriptor limit, add the following lines to the `/etc/security/limits.conf` file:

```bash
ydb soft nofile 10000
ydb hard nofile 10000
```

Where `ydb` is the username under which `ydbd` runs.

After changing the file, you need to reboot the system or log in again to apply the new limits.

{% note info %}

For more information about spilling configuration and its relationship with file descriptors, see the [Spilling Configuration](../../../reference/configuration/table_service_config.md#file-system-requirements) section.

{% endnote %}

## Install {{ ydb-short-name }} Software on Each Server {#install-binaries}

1. Download and unpack an archive with the `ydbd` executable and the libraries required for {{ ydb-short-name }} to run:

    ```bash
    mkdir ydbd-stable-linux-amd64
    curl -L <binaries_url> | tar -xz --strip-component=1 -C ydbd-stable-linux-amd64
    ```
    where `binaries_url` is a link to the archive of the version you need from the [downloads](../../../downloads/index.md) page.

1. Copy the executable and libraries to the appropriate directories:

    ```bash
    sudo cp -iR ydbd-stable-linux-amd64/bin /opt/ydb/
    sudo cp -iR ydbd-stable-linux-amd64/lib /opt/ydb/
    ```

1. Set the owner of files and directories:

    ```bash
    sudo chown -R root:bin /opt/ydb
    ```
## Prepare and Clear Disks on Each Server {#prepare-disks}

{% include [_includes/storage-device-requirements.md](../../../_includes/storage-device-requirements.md) %}

To get a list of block devices on the server, use the `lsblk` command. Example output:

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

The names of block devices depend on the operating system settings, defined by the base image or configured manually. Typically, device names consist of three parts:

- A fixed prefix or a prefix indicating the device type
- A sequential device identifier (can be a letter or a number)
- A sequential partition identifier on that device (usually a number)

1. Create partitions on the selected disks:

    {% note alert %}

    The following operation will delete all partitions on the specified disk! Make sure you specified a disk that has no other data!

    {% endnote %}

    ```bash
    DISK=/dev/nvme0n1
    sudo parted ${DISK} mklabel gpt -s
    sudo parted -a optimal ${DISK} mkpart primary 0% 100%
    sudo parted ${DISK} name 1 ydb_disk_ssd_01
    sudo partx --u ${DISK}
    ```

    Execute the command `ls -l /dev/disk/by-partlabel/` to ensure that a disk with the label `/dev/disk/by-partlabel/ydb_disk_ssd_01` has appeared in the system.

    If you plan to use more than one disk on each server, specify a unique label for each instead of `ydb_disk_ssd_01`. Disk labels must be unique within each server and are used in configuration files, as shown in the following instructions.

    To simplify subsequent configuration, it is convenient to use the same disk labels on cluster servers with identical disk configurations.

2. Clear the disk using the command built into the `ydbd` executable:

    {% note warning %}

    After executing the command, data on the disk will be erased.

    {% endnote %}

    ```bash
    sudo LD_LIBRARY_PATH=/opt/ydb/lib /opt/ydb/bin/ydbd admin bs disk obliterate /dev/disk/by-partlabel/ydb_disk_ssd_01
    ```

    Perform this operation for each disk that will be used for {{ ydb-short-name }} data storage.

### Example of a Complete Command for Partitioning 3 Disks

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

### Check Disk Preparation

To verify correct disk partitioning, run the following command on each cluster server:

```bash
ls -al /dev/disk/by-partlabel/
```

The command output should include the disks you created and partitioned:

```bash
lrwxrwxrwx 1 root root    10 Nov 26 12:54 ydb_disk_ssd_01 -> ../../vdb1
lrwxrwxrwx 1 root root    10 Nov 26 12:54 ydb_disk_ssd_02 -> ../../vdc1
lrwxrwxrwx 1 root root    10 Nov 26 12:54 ydb_disk_ssd_03 -> ../../vdd1
```
## Prepare Configuration Files {#config}

Prepare a configuration file for {{ ydb-short-name }}:
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

To speed up and simplify the initial deployment of {{ ydb-short-name }}, the configuration file already contains most of the settings for setting up the cluster. It is sufficient to replace the standard host FQDNs with the actual ones in the `hosts` and `blob_storage_config` sections.

* The `hosts` section:

  ```yaml
  ...
  hosts:
    - host: static-node-1.ydb-cluster.com # VM FQDN
      host_config_id: 1
      walle_location:
        body: 1
        data_center: 'zone-a'
        rack: '1'
  ...
  ```

* The `blob_storage_config` section:

  ```yaml
  ...
  - fail_domains:
    - vdisk_locations:
      - node_id: static-node-1.ydb-cluster.com # VM FQDN
        pdisk_category: SSD
        path: /dev/disk/by-partlabel/ydb_disk_1
  ...
  ```

The other sections and settings of the configuration file remain unchanged.

Save the YDB configuration file as `/opt/ydb/cfg/config.yaml` on each cluster server.

For more detailed information on creating the configuration file, see [{#T}](../../../reference/configuration/index.md).
## Copy the TLS Keys and Certificates to Each Server {#tls-copy-cert}

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

## Start Static Nodes {#start-storage}

{% list tabs group=manual-systemd %}

- Manually

  Run a {{ ydb-short-name }} data storage service on each static cluster node:

  ```bash
  sudo su - ydb
  cd /opt/ydb
  export LD_LIBRARY_PATH=/opt/ydb/lib
  /opt/ydb/bin/ydbd server --log-level 3 --syslog --tcp --config-dir /opt/ydb/cfg \
      --grpcs-port 2135 --ic-port 19001 --mon-port 8765 --mon-cert /opt/ydb/certs/web.pem --node static
  ```

- Using systemd

  On each server that will host a static cluster node, create a systemd configuration file `/etc/systemd/system/ydbd-storage.service` by the template below. You can also [download the sample file from the repository](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/systemd_services/ydbd-storage.service).

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

After starting the static nodes, check their operability via the built-in {{ ydb-short-name }} web interface (Embedded UI):

1. Open the address `https://<node.ydb.tech>:8765` in a browser, where `<node.ydb.tech>` is the FQDN of the server where any static node is running;
2. Go to the **Nodes** tab;
3. Make sure that all 3 static nodes are displayed in the list.

![Manual installation, running static nodes](../_assets/manual_installation_1.png)
## Initialize a Cluster {#initialize-cluster}

The cluster initialization operation sets up the set of static nodes listed in the cluster configuration file for storing {{ ydb-short-name }} data.

To initialize the cluster, you will need the Certificate Authority certificate file `ca.crt`, the path to which must be specified when executing the corresponding commands. Before executing the commands, copy the `ca.crt` file to the server where these commands will be executed.

On one of the storage servers in the cluster, run the commands:

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

If the cluster initialization is successful, the exit code of the cluster initialization command displayed on the screen should be zero.
## Create a Database {#create-db}

To work with [row-oriented](../../../concepts/datamodel/table.md#row-oriented-tables) and [column-oriented](../../../concepts/datamodel/table.md#column-oriented-tables) tables, you need to create at least one database and run a process (or processes) to serve this database (dynamic nodes).

To execute the administrative command for database creation, you will need the `ca.crt` certificate file issued by the Certificate Authority (see the above description of cluster initialization).

When creating your database, you set an initial number of storage groups that determine the available input/output throughput and maximum storage. For an existing database, you can increase the number of storage groups when needed.

On one of the storage servers in the cluster, run the following commands:

```bash
export LD_LIBRARY_PATH=/opt/ydb/lib
/opt/ydb/bin/ydbd --ca-file ca.crt -s grpcs://`hostname -f`:2135 -f auth_token \
    admin database /Root/testdb create ssd:8
echo $?
```

You will see that the database was created successfully when the command returns a zero code.

The command example above uses the following parameters:

* `/Root`: Name of the root domain, automatically generated upon cluster initialization.
* `testdb`: Name of the created database.
* `ssd:8`: Defines the storage pool for the database and the number of groups in it. The pool name (`ssd`) must correspond to the disk type specified in the cluster configuration (for example, in `default_disk_type`) and is case-insensitive. The number after the colon is the number of storage groups to be allocated.
## Run Dynamic Nodes {#start-dynnode}

{% list tabs group=manual-systemd %}

- Manually

  Run the {{ ydb-short-name }} dynamic node for the `/Root/testdb` database:

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

  In the command example above, `<ydb-static-node1>`, `<ydb-static-node2>`, `<ydb-static-node3>` are the FQDNs of any three servers running the cluster's static nodes.

- Using systemd

  Create a systemd configuration file named `/etc/systemd/system/ydbd-testdb.service` by the following template: You can also [download](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/systemd_services/ydbd-testdb.service) the sample file from the repository.

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

  In the file example above, `<ydb-static-node1>`, `<ydb-static-node2>`, `<ydb-static-node3>` are the FQDNs of any three servers running the cluster's static nodes.

  Run the {{ ydb-short-name }} dynamic node for the `/Root/testdb` database:

  ```bash
  sudo systemctl start ydbd-testdb
  ```

{% endlist %}

Run additional dynamic nodes on other servers to ensure database scalability and fault tolerance.
## Initial Account Setup {#security-setup}

1. Set the password for the `root` account using the previously obtained token:

    ```bash
    ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --token-file auth_token \
        yql -s 'ALTER USER root PASSWORD "passw0rd"'
    ```

    Replace the value `passw0rd` with the required password. Save the password in a separate file. Subsequent commands as the `root` user will be executed using the password passed with the `--password-file <path_to_user_password>` option. Additionally, the password can be saved in the connection profile, as described in the [{{ ydb-short-name }} CLI documentation](../../../reference/ydb-cli/profile/index.md).

1. Create additional accounts:

    ```bash
    ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root --password-file <path_to_root_pass_file> \
        yql -s 'CREATE USER user1 PASSWORD "passw0rd"'
    ```

1. Set account rights by including them in the built-in groups:

    ```bash
    ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root --password-file <path_to_root_pass_file> \
        yql -s 'ALTER GROUP `ADMINS` ADD USER user1'
    ```

In the command examples listed above, `<node.ydb.tech>` is the FQDN of the server where any dynamic node servicing the `/Root/testdb` database is running. When connecting via SSH to a {{ ydb-short-name }} dynamic node, it's convenient to use the `grpcs://$(hostname -f):2136` construct to get the FQDN.

When executing account creation and group assignment commands, the {{ ydb-short-name }} CLI client will prompt for the `root` user password. To avoid repeated password entry, you can create a connection profile, as described in the [{{ ydb-short-name }} CLI documentation](../../../reference/ydb-cli/profile/index.md).
## Start Using the Created Database {#try-first-db}

1. Install the {{ ydb-short-name }} CLI as described in the [documentation](../../../reference/ydb-cli/install.md).

1. Create a test row (`test_row_table`) or column (`test_column_table`) oriented table:

{% list tabs %}

- Creating a row-oriented table

   ```bash
   ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root \
      yql -s 'CREATE TABLE `testdir/test_row_table` (id Uint64, title Utf8, PRIMARY KEY (id));'
   ```

- Creating a column-oriented table

   ```bash
   ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root \
      yql -s 'CREATE TABLE `testdir/test_column_table` (id Uint64 NOT NULL, title Utf8, PRIMARY KEY (id)) WITH (STORE = COLUMN);'
   ```

{% endlist %}

Here, `<node.ydb.tech>` is the FQDN of the server running the dynamic node that serves the `/Root/testdb` database.


## Checking Access to the Built-in Web Interface

To check access to the {{ ydb-short-name }} built-in web interface, open in the browser the `https://<node.ydb.tech>:8765` URL, where `<node.ydb.tech>` is the FQDN of the server running any static {{ ydb-short-name }} node.

In the web browser, set as trusted the certificate authority that issued certificates for the {{ ydb-short-name }} cluster. Otherwise, you will see a warning about an untrusted certificate.

If authentication is enabled in the cluster, the web browser should prompt you for a login and password. Enter your credentials, and you'll see the built-in interface welcome page. The user interface and its features are described in [{#T}](../../../reference/embedded-ui/index.md).

{% note info %}

A common way to provide access to the {{ ydb-short-name }} built-in web interface is to set up a fault-tolerant HTTP balancer running `haproxy`, `nginx`, or similar software. A detailed description of the HTTP balancer is beyond the scope of the standard {{ ydb-short-name }} installation guide.

{% endnote %}

## Installing {{ ydb-short-name }} in the Unprotected Mode

{% note warning %}

We do not recommend using the unprotected {{ ydb-short-name }} mode for development or production environments.

{% endnote %}

The above installation procedure assumes that {{ ydb-short-name }} was deployed in the standard protected mode.

The unprotected {{ ydb-short-name }} mode is primarily intended for test scenarios associated with {{ ydb-short-name }} software development and testing. In the unprotected mode:

* Traffic between cluster nodes and between applications and the cluster runs over an unencrypted connection.
* Users are not authenticated (it doesn't make sense to enable authentication when the traffic is unencrypted because the login and password in such a configuration would be transmitted across the network in plain text).

When installing {{ ydb-short-name }} to run in the unprotected mode, follow the above procedure, with the following exceptions:

1. When preparing for the installation, you do not need to generate TLS certificates and keys and copy the certificates and keys to the cluster nodes.
1. In the configuration files, remove the `security_config`, `interconnect_config`, and `grpc_config` sections entirely.
1. Use simplified commands to run static and dynamic cluster nodes: omit the options that specify file names for certificates and keys; use the `grpc` protocol instead of `grpcs` when specifying the connection points.
1. Skip the step of obtaining an authentication token before cluster initialization and database creation because it's not needed in the unprotected mode.
1. Cluster initialization command has the following format:

    ```bash
    export LD_LIBRARY_PATH=/opt/ydb/lib
    ydb admin cluster bootstrap --uuid <string>
    echo $?
    ```

1. Database creation command has the following format:

    ```bash
    export LD_LIBRARY_PATH=/opt/ydb/lib
    /opt/ydb/bin/ydbd admin database /Root/testdb create ssd:1
    ```

1. When accessing your database from the {{ ydb-short-name }} CLI and applications, use grpc instead of grpcs and skip authentication.
