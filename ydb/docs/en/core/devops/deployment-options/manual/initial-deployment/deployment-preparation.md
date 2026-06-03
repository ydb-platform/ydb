# Preparing for deployment

## Before you begin {#before-start}

### Requirements {#requirements}

Review the [system requirements](../../../../devops/concepts/system-requirements.md) and [cluster topology](../../../../concepts/topology.md).

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

More details about hardware requirements are described in the section [{#T}](../../../../devops/concepts/system-requirements.md).

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

For proper operation of {{ ydb-short-name }}, especially when using [spilling](../../../../concepts/query_execution/spilling.md) in multi-node clusters, it is recommended to increase the limit on the number of simultaneously open file descriptors.

To change the file descriptor limit, add the following lines to the `/etc/security/limits.conf` file:

```bash
ydb soft nofile 10000
ydb hard nofile 10000
```

Where `ydb` is the name of the user under which `ydbd` is run.

After modifying the file, you need to reboot the system or log in again to apply the new limits.

{% note info %}

For more information about spilling configuration and its relation to file descriptors, see the ["Spilling configuration"](../../../../reference/configuration/table_service_config.md#file-system-requirements) section.

{% endnote %}

## Install {{ ydb-short-name }} software on each server {#install-binaries}

### Download and unpack the archive containing the `ydbd` executable and the {{ ydb-short-name }} libraries required for operation

{% list tabs %}

- OSS

  ```bash
  mkdir ydbd-stable-linux-amd64
  curl -L <binaries_url> | tar -xz --strip-component=1 -C ydbd-stable-linux-amd64
  ```

- Enterprise

  ```bash
  mkdir ydbd-stable-linux-amd64
  curl -L <binaries_url> | tar -xJ --strip-component=1 -C ydbd-stable-linux-amd64
  ```

{% endlist %}

where `binaries_url` is a link to the archive of the version you need from the [downloads](../../../../downloads/index.md) page.

### Create a directory on the server

```bash
sudo mkdir -p  /opt/ydb
```

### Copy the executable file and libraries to the appropriate directories

```bash
sudo cp -iR ydbd-stable-linux-amd64/bin /opt/ydb/
sudo cp -iR ydbd-stable-linux-amd64/lib /opt/ydb/
```

### Set the owner of files and directories

```bash
sudo chown -R root:bin /opt/ydb
```

## Prepare and clean disks on each server {#prepare-disks}

{% include [_includes/storage-device-requirements.md](../../../../_includes/storage-device-requirements.md) %}

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

* A fixed prefix or a prefix indicating the device type
* A sequential device identifier (can be a letter or a number)
* A sequential partition identifier on the device (usually a number)

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

After completing the preparatory steps, you can proceed to deploying the system. Choose the instructions according to your configuration:

* [Deploying a cluster using V1 configuration](deployment-configuration-v1.md)
* [Deploying a cluster using V2 configuration](deployment-configuration-v2.md)
