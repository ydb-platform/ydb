# Deployment Preparation

## Before You Start {#before-start}

### Requirements {#requirements}

Review the [system requirements](../../../../devops/concepts/system-requirements.md) and [cluster topology](../../../../concepts/topology.md).

You must have SSH access to all servers. This is required to install the artifacts and run the {{ ydb-short-name }} executable file.

The network configuration must allow TCP connections on the following ports (default, can be changed by settings):

* 22: SSH service;
* 2135, 2136: gRPC for client-cluster interaction;
* 19001, 19002: Interconnect for intra-cluster node interaction;
* 8765, 8766: HTTP interface of {{ ydb-short-name }} Embedded UI;
* 9092, 9093: ports for Kafka API operations.

If you place multiple dynamic nodes on one server, you will need separate ports for gRPC, Interconnect, the HTTP interface, and Kafka API for each dynamic node on the server.

Make sure that the system clocks on all servers in the cluster are synchronized using `ntpd` or `chrony` tools. It is advisable to use a single time source for all cluster servers to ensure consistent handling of leap seconds.

If the Linux type used on the cluster servers uses `syslogd` for logging, you must set up log rotation using the `logrotate` tool or its analogs. {{ ydb-short-name }} services can generate a significant amount of system logs, especially when increasing the logging level for diagnostic purposes, so it is important to enable system log rotation to prevent `/var` filesystem overflow.

Select the servers and disks that will be used for data storage:

* Use the `block-4-2` fault tolerance scheme to deploy a cluster in a single availability zone (AZ), using at least 8 servers. This scheme allows you to withstand the failure of 2 servers.
* Use the `mirror-3-dc` fault tolerance scheme to deploy a cluster in three availability zones (AZs), using at least 9 servers. This scheme allows you to withstand the failure of 1 AZ and 1 server in another AZ. The number of servers used in each AZ must be the same.

{% note info %}

Run each static node (data storage node) on a separate server. It is possible to combine static and dynamic nodes on one server, as well as place multiple dynamic nodes on one server if there are sufficient computing resources.

{% endnote %}

For more details on hardware requirements, see the [section](../../../../devops/concepts/system-requirements.md).

### Preparing TLS Keys and Certificates {#tls-certificates}

Traffic protection and authentication of {{ ydb-short-name }} server nodes are implemented using the TLS protocol. Before installing the cluster, you must plan the server composition, decide on the node naming scheme and specific names, and prepare TLS keys and certificates.

You can use existing certificates or generate new ones. The following TLS key and certificate files must be prepared in PEM format:

* `ca.crt` — the certificate of the certification authority (CA) that signs the other TLS certificates (the same files on all cluster nodes);
* `node.key` — TLS secret keys for each of the cluster nodes (a separate key for each cluster server);
* `node.crt` — TLS certificates for each of the cluster nodes (a certificate corresponding to the key);
* `web.pem` — concatenation of the node's secret key, node certificate, and CA certificate for the monitoring HTTP interface (a separate file for each cluster server).

The required parameters for generating certificates are determined by the organization's policy. Typically, certificates and keys for {{ ydb-short-name }} are generated with the following parameters:

* RSA keys with a length of 2048 or 4096 bits;
* SHA-256 signature algorithm with RSA encryption;
* node certificates' validity period of at least 1 year;
* CA certificate's validity period of at least 3 years.

The CA certificate must be marked accordingly: the CA flag must be set, and the following usages must be enabled: "Digital Signature, Non Repudiation, Key Encipherment, Certificate Sign".

For node certificates, it is important that the actual hostname (or hostnames) matches the values specified in the "Subject Alternative Name" field. The certificates must have the following usages enabled: "Digital Signature, Key Encipherment" and extended usages "TLS Web Server Authentication, TLS Web Client Authentication". It is necessary that node certificates support both server and client authentication (the `extendedKeyUsage = serverAuth,clientAuth` option in OpenSSL settings).

To batch generate or update {{ ydb-short-name }} cluster certificates using OpenSSL software, you can use the [script example](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/tls_cert_gen/) from the {{ ydb-short-name }} repository on GitHub. The script allows you to automatically generate the necessary key and certificate files for the entire set of cluster nodes in one operation, simplifying the preparation for installation.

## Create a System User and Group for {{ ydb-short-name }} {#create-user}

On each server where {{ ydb-short-name }} will be running, execute:

```bash
sudo groupadd ydb
sudo useradd ydb -g ydb
```

To allow the {{ ydb-short-name }} service to access block disks, you must add the user under which the {{ ydb-short-name }} processes will run to the `disk` group:

```bash
sudo usermod -aG disk ydb
```

## Configure File Descriptor Limits {#file-descriptors}

For {{ ydb-short-name }} to operate correctly, especially when using [spilling](../../../../concepts/query_execution/spilling.md) in multi-node clusters, it is recommended to increase the limit on the number of simultaneously open file descriptors.

To change the file descriptor limit, add the following lines to the `/etc/security/limits.conf` file:

```bash
ydb soft nofile 10000
ydb hard nofile 10000
```

Where `ydb` is the username under which `ydbd` is running.

After modifying the file, you must reboot the system or log in again to apply the new limits.

{% note info %}

For more information about spilling configuration and its relation to file descriptors, see the [«Spilling Configuration»](../../../../reference/configuration/table_service_config.md#file-system-requirements) section.

{% endnote %}

## Install {{ ydb-short-name }} Software on Each Server {#install-binaries}

### Download and Extract the Archive with the `ydbd` Executable and Required Libraries

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

where `binaries_url` is the link to the archive of the desired version from the [downloads page](../../../../downloads/index.md).

### Create a Directory on the Server

  ```bash
  sudo mkdir -p  /opt/ydb
  ```

### Copy the Executable and Libraries to the Appropriate Directories

  ```bash
  sudo cp -iR ydbd-stable-linux-amd64/bin /opt/ydb/
  sudo cp -iR ydbd-stable-linux-amd64/lib /opt/ydb/
  ```

### Set the Owner of Files and Directories

  ```bash
  sudo chown -R root:bin /opt/ydb
  ```

## Prepare and Clean Disks on Each Server {#prepare-disks}

{% include [_includes/storage-device-requirements.md](../../../../_includes/storage-device-requirements.md) %}

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

The names of block devices depend on the operating system settings, set by the base image or configured manually. Typically, device names consist of three parts:

* A fixed prefix or a prefix indicating the device type;
* A sequential device identifier (can be a letter or a number);
* A sequential partition identifier on the given device (usually a number).

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

    Run the `ls -l /dev/disk/by-partlabel/` command to make sure that a disk with the label `/dev/disk/by-partlabel/ydb_disk_ssd_01` has appeared in the system.

    If you plan to use more than one disk on each server, specify a unique label for each disk instead of `ydb_disk_ssd_01`. Disk labels must be unique within each server and are used in configuration files, as shown in the following instructions.

    To simplify subsequent configuration, it is convenient to use the same disk labels on cluster servers with identical disk configurations.

2. Clean the disk using the command built into the `ydbd` executable:

    {% note warning %}

    Data on the disk will be erased after executing the command.

    {% endnote %}

    ```bash
    sudo LD_LIBRARY_PATH=/opt/ydb/lib /opt/ydb/bin/ydbd admin bs disk obliterate /dev/disk/by-partlabel/ydb_disk_ssd_01
    ```

    Perform this operation for each disk that will be used for {{ ydb-short-name }} data storage.

### Example of a Full Command for Partitioning 3 Disks

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

### Verify Disk Preparation

To check the correct partitioning of disks, run the command on each cluster server:

```bash
ls -al /dev/disk/by-partlabel/
```

The command output should include the disks you created and partitioned:

```bash
lrwxrwxrwx 1 root root    10 Nov 26 12:54 ydb_disk_ssd_01 -> ../../vdb1
lrwxrwxrwx 1 root root    10 Nov 26 12:54 ydb_disk_ssd_02 -> ../../vdc1
lrwxrwxrwx 1 root root    10 Nov 26 12:54 ydb_disk_ssd_03 -> ../../vdd1
```

After completing the preparatory steps, you can proceed with system deployment. Choose the instruction according to your configuration:

* [Deploying a Cluster Using Configuration V1](deployment-configuration-v1.md)
* [Deploying a Cluster Using Configuration V2](deployment-configuration-v2.md)
