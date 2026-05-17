# Deployment Preparation

## Getting Started {#before-start}

### Prerequisites {#requirements}

Review the [system requirements](../../../../devops/concepts/system-requirements.md) and the [cluster topology](../../../../concepts/topology.md).

Make sure you have SSH access to all servers. This is required to install artifacts and run the {{ ydb-short-name }} executable.

The network configuration must allow TCP connections on the following ports (these are defaults, but you can change them through configuration):

* 22: SSH service
* 2135, 2136: gRPC for client-cluster interaction.
* 19001, 19002: Interconnect for intra-cluster node interaction
* 8765, 8766: HTTP interface of {{ ydb-short-name }} Embedded UI.
* 9092, 9093: ports for Kafka API.

Distinct ports are necessary for gRPC, Interconnect, HTTP interface and Kafka API of each dynamic node when hosting multiple dynamic nodes on a single server.

Make sure that the system clocks running on all the cluster's servers are synced by `ntpd` or `chrony`. We recommend using the same time source for all servers in the cluster to maintain consistent leap seconds processing.

If the Linux distribution used on the cluster servers uses `syslogd` for logging, set up log file rotation using `logrotate` or similar tools. {{ ydb-short-name }} services can generate substantial amounts of system logs, particularly when you elevate the logging level for diagnostic purposes. That's why it's important to enable system log file rotation to prevent the `/var` file system overflow.

Select the servers and disks to be used for storing data:

* Use the `block-4-2` fault tolerance model for cluster deployment in one availability zone (AZ). Use at least eight servers to safely survive the loss of two servers.
* Use the `mirror-3-dc` fault tolerance model for cluster deployment in three availability zones (AZ). To survive the loss of one AZ and one server in another AZ, use at least nine servers. Make sure that the number of servers running in each AZ is the same.

{% note info %}

Run each static node (data node) on a separate server. Both static and dynamic nodes can run together on the same server. A server can also run multiple dynamic nodes if it has enough computing power.

{% endnote %}

For more information about hardware requirements, see [{#T}](../../../../devops/concepts/system-requirements.md).

### Preparing TLS Keys and Certificates {#tls-certificates}

The TLS protocol provides traffic protection and authentication for {{ ydb-short-name }} server nodes. Before you install your cluster, determine which servers it will host, establish the node naming convention, come up with node names, and prepare your TLS keys and certificates.

You can use existing certificates or generate new ones. Prepare the following files with TLS keys and certificates in the PEM format:

* `ca.crt`: CA certificate used to sign the other TLS certificates (these files are the same on all the cluster nodes).
* `node.key`: Secret TLS keys for each cluster node (one key per cluster server).
* `node.crt`: TLS certificates for each cluster node (each certificate corresponds to a key).
* `web.pem`: Concatenation of the node secret key, node certificate, and the CA certificate needed for the monitoring HTTP interface (a separate file is used for each server in the cluster).

Your organization should define the parameters required for certificate generation in its policy. The following parameters are commonly used for generating certificates and keys for {{ ydb-short-name }}:

* 2048-bit or 4096-bit RSA keys
* Certificate signing algorithm: SHA-256 with RSA encryption
* Validity period of node certificates: at least 1 year
* CA certificate validity period: at least 3 years.

Make sure that the CA certificate is appropriately labeled, with the CA property enabled along with the "Digital Signature, Non Repudiation, Key Encipherment, Certificate Sign" usage types.

For node certificates, it is important that the actual host name (or names) match the values in the "Subject Alternative Name" field. Enable both the regular usage types ("Digital Signature, Key Encipherment") and advanced usage types ("TLS Web Server Authentication, TLS Web Client Authentication") for the certificates. Node certificates must support both server authentication and client authentication (the `extendedKeyUsage = serverAuth,clientAuth` option in the OpenSSL settings).

For batch generation or update of {{ ydb-short-name }} cluster certificates using OpenSSL, you can use the [sample script](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/tls_cert_gen/) from the {{ ydb-short-name }} GitHub repository. Using the script, you can streamline preparation for installation, automatically generating all the key files and certificate files for all your cluster nodes in a single step.
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

For proper operation of {{ ydb-short-name }}, especially when using [spilling](../../../../concepts/query_execution/spilling.md) in multi-node clusters, it is recommended to increase the limit of simultaneously open file descriptors.

To change the file descriptor limit, add the following lines to the `/etc/security/limits.conf` file:

```bash
ydb soft nofile 10000
ydb hard nofile 10000
```

Where `ydb` is the username under which `ydbd` runs.

After changing the file, you need to reboot the system or log in again to apply the new limits.

{% note info %}

For more information about spilling configuration and its relationship with file descriptors, see the [Spilling Configuration](../../../../reference/configuration/table_service_config.md#file-system-requirements) section.

{% endnote %}

## Install {{ ydb-short-name }} Software on Each Server {#install-binaries}

### Download and unpack an archive with the `ydbd` executable and the libraries required for {{ ydb-short-name }} to run

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

  where `binaries_url` is the link to the archive of the version you need from the [downloads](../../../../downloads/index.md) page.

### Create a directory on the server

  ```bash
  sudo mkdir -p /opt/ydb
  ```

### Copy the executable and libraries to the appropriate directories

  ```bash
  sudo cp -iR ydbd-stable-linux-amd64/bin /opt/ydb/
  sudo cp -iR ydbd-stable-linux-amd64/lib /opt/ydb/
  ```

### Set the owner of files and directories

  ```bash
  sudo chown -R root:bin /opt/ydb
  ```
## Prepare and Clear Disks on Each Server {#prepare-disks}

{% include [_includes/storage-device-requirements.md](../../../../_includes/storage-device-requirements.md) %}

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

Block device names depend on the operating system settings provided by the base image or configured manually. Typically, device names consist of three parts:

* A fixed prefix or a prefix indicating the device type
* A sequential device identifier (can be a letter or a number)
* A sequential partition identifier on that device (usually a number)

1. Create partitions on the selected disks:

    {% note alert %}

    The following operation will delete all partitions on the specified disk! Make sure you have specified a disk that does not contain any other data!

    {% endnote %}

    ```bash
    DISK=/dev/nvme0n1
    sudo parted ${DISK} mklabel gpt -s
    sudo parted -a optimal ${DISK} mkpart primary 0% 100%
    sudo parted ${DISK} name 1 ydb_disk_ssd_01
    sudo partx --u ${DISK}
    ```

    Run the command `ls -l /dev/disk/by-partlabel/` to verify that a disk with the label `/dev/disk/by-partlabel/ydb_disk_ssd_01` has appeared in the system.

    If you plan to use more than one disk on each server, specify a unique label for each instead of `ydb_disk_ssd_01`. Disk labels must be unique within each server and are used in configuration files, as shown in the subsequent instructions.

    To simplify subsequent configuration, it is convenient to use the same disk labels on cluster servers with identical disk configurations.

2. Clear the disk using the command built into the `ydbd` executable:

    {% note warning %}

    After executing the command, data on the disk will be erased.

    {% endnote %}

    ```bash
    sudo LD_LIBRARY_PATH=/opt/ydb/lib /opt/ydb/bin/ydbd admin bs disk obliterate /dev/disk/by-partlabel/ydb_disk_ssd_01
    ```

    Perform this operation for each disk that will be used for {{ ydb-short-name }} data storage.

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

After completing the preparatory steps, you can proceed to deploy the system. Select the instruction according to your configuration:

* [Deploying a cluster using configuration V1](deployment-configuration-v1.md)
* [Deploying a cluster using configuration V2](deployment-configuration-v2.md)
