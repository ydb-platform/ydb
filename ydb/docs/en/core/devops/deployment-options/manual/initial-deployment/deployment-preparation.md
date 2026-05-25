# Deployment preparation
## Before starting work {#before-start}
### Requirements {#requirements}

Familiarize yourself with the [system requirements](../../../../devops/concepts/system-requirements.md) and [cluster topology](../../../../concepts/topology.md).

You must have SSH access to all servers. This is necessary for installing artifacts and running the {{ ydb-short-name }} executable file.

The network configuration must allow TCP connections on the following ports (default, can be changed by settings):

* 22: SSH service;
* 2135, 2136: gRPC for client-cluster interaction;
* 19001, 19002: Interconnect for intra-cluster node interaction;
* 8765, 8766: HTTP interface of {{ ydb-short-name }} Embedded UI.
* 9092, 9093: ports for working with Kafka API.

When placing multiple dynamic nodes on one server, separate ports for gRPC, Interconnect, HTTP interface, and Kafka API are required for each dynamic node within the server.

Make sure that the system clocks on all servers in the cluster are synchronized using `ntpd` or `chrony` tools. It is advisable to use a single time source for all cluster servers to ensure consistent handling of leap seconds.

If the Linux type used on the cluster servers uses `syslogd` for logging, it is necessary to configure log file rotation using the `logrotate` tool or its analogs. {{ ydb-short-name }} services can generate a significant amount of system logs, especially when increasing the logging level for diagnostic purposes, so it is important to enable system log file rotation to prevent /var filesystem overflow.

Select the servers and disks that will be used for data storage:

* Use the `block-4-2` fault tolerance scheme to deploy the cluster in a single availability zone (AZ), using at least 8 servers. This scheme allows you to withstand the failure of 2 servers.
* Use the `mirror-3-dc` fault tolerance scheme to deploy the cluster in three availability zones (AZ), using at least 9 servers. This scheme allows you to withstand the failure of 1 AZ and 1 server in another AZ. The number of servers involved in each AZ must be the same.
{% note info %}

Run each static node (data storage node) on a separate server. It is possible to combine static and dynamic nodes on the same server, as well as host multiple dynamic nodes on a single server if there are sufficient computing resources.

{% endnote %}
More about hardware requirements is described in the section [{#T}](../../../../devops/concepts/system-requirements.md).
### Preparing TLS Keys and Certificates {#tls-certificates}

Traffic protection and verification of the authenticity of {{ ydb-short-name }} server nodes is carried out using the TLS protocol. Before installing the cluster, you need to plan the composition of the servers, decide on the node naming scheme and specific names, and prepare TLS keys and certificates.

You can use existing ones or generate new certificates. The following TLS key and certificate files must be prepared in PEM format:

* `ca.crt` — the certificate of the certification authority (CA) that signs the other TLS certificates (the same files on all cluster nodes);
* `node.key` — TLS private keys for each of the cluster nodes (a separate key for each cluster server);
* `node.crt` — TLS certificates for each of the cluster nodes (a certificate corresponding to the key);
* `web.pem` — a concatenation of the node's private key, the node certificate, and the CA certificate for the HTTP monitoring interface (a separate file for each cluster server).

The required parameters for generating certificates are determined by the organization's policy. Typically, certificates and keys for {{ ydb-short-name }} are generated with the following parameters:

* RSA keys with a length of 2048 or 4096 bits;
* certificate signing algorithm SHA-256 with RSA encryption;
* the validity period of the node certificates is at least 1 year;
* the validity period of the CA certificate is at least 3 years.

It is necessary that the CA certificate is marked accordingly: the CA flag must be set, and the following uses must be enabled: "Digital Signature, Non Repudiation, Key Encipherment, Certificate Sign".

For node certificates, it is important that the actual host name (or host names) matches the values specified in the "Subject Alternative Name" field. The certificates must include the following uses: "Digital Signature, Key Encipherment" and extended uses: "TLS Web Server Authentication, TLS Web Client Authentication". It is necessary that the node certificates support both server and client authentication (the `extendedKeyUsage = serverAuth,clientAuth` option in OpenSSL settings).

To batch generate or update {{ ydb-short-name }} cluster certificates using OpenSSL software, you can use the [sample script](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/tls_cert_gen/) located in the {{ ydb-short-name }} repository on GitHub. The script allows you to automatically generate the necessary key and certificate files for the entire set of cluster nodes in a single operation, simplifying preparation for installation.
## Create a system user and group under which {{ ydb-short-name }} will run {#create-user}

On each server where {{ ydb-short-name }} will be run, do the following:
```bash
sudo groupadd ydb
sudo useradd ydb -g ydb
```
In order for the {{ ydb-short-name }} service to have access to block devices for operation, it is necessary to add the user under which the {{ ydb-short-name }} processes will run to the `disk` group:
```bash
sudo usermod -aG disk ydb
```
## Set file descriptor limits {#file-descriptors}

For {{ ydb-short-name }} to operate correctly, especially when using [spilling](../../../../concepts/query_execution/spilling.md) in multi-node clusters, it is recommended to increase the limit on the number of simultaneously open file descriptors.

To change the file descriptor limit, add the following lines to the `/etc/security/limits.conf` file:
```bash
ydb soft nofile 10000
ydb hard nofile 10000
```
Where `ydb` is the username under which `ydbd` is run.

After changing the file, you need to reboot the system or log in again to apply the new limits.
{% note info %}

For more information about spilling configuration and its relation to file descriptors, see the section [«Spilling Configuration»](../../../../reference/configuration/table_service_config.md#file-system-requirements).

{% endnote %}
## Install {{ ydb-short-name }} software on each server {#install-binaries}
### Download and unpack the archive with the `ydbd` executable file and the libraries required for {{ ydb-short-name }} to work

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
## Prepare and clean up disks on each server {#prepare-disks}

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
The names of block devices depend on the operating system settings, either set by the base image or configured manually. Typically, device names consist of three parts:

* A fixed prefix or a prefix indicating the device type
* A sequential device identifier (can be a letter or a number)
* A sequential partition identifier on the given device (usually a number)

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
Run the command `ls -l /dev/disk/by-partlabel/` to make sure that the disk with the label `/dev/disk/by-partlabel/ydb_disk_ssd_01` has appeared in the system.

If you plan to use more than one disk on each server, specify a unique label for each one instead of `ydb_disk_ssd_01`. Disk labels must be unique within each server and are used in configuration files, as shown in the following instructions.

To simplify subsequent configuration, it is convenient to use the same disk labels on cluster servers with identical disk configurations.

2. Clear the disk using the command built into the `ydbd` executable file:
{% note warning %}

The data on the disk will be erased after running the command.

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
### Check disk preparation

To verify that the disk partitioning is correct, run the command on each cluster server:
```bash
ls -al /dev/disk/by-partlabel/
```
The command output should include the disks you have created and labeled
```bash
lrwxrwxrwx 1 root root    10 Nov 26 12:54 ydb_disk_ssd_01 -> ../../vdb1
lrwxrwxrwx 1 root root    10 Nov 26 12:54 ydb_disk_ssd_02 -> ../../vdc1
lrwxrwxrwx 1 root root    10 Nov 26 12:54 ydb_disk_ssd_03 -> ../../vdd1
```
After completing the preparatory activities, you can proceed with deploying the system. Select the instruction according to your configuration:

* [Deploying a cluster using configuration V1](deployment-configuration-v1.md)
* [Deploying a cluster using configuration V2](deployment-configuration-v2.md)
