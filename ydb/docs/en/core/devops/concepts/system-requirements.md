# System Requirements and Recommendations for {{ ydb-short-name }}

This section provides recommendations for deploying {{ ydb-short-name }} clusters, applicable regardless of the chosen infrastructure management method ([Ansible](../deployment-options/ansible/index.md), [Kubernetes](../deployment-options/kubernetes/index.md), or [manual](../deployment-options/manual/index.md)).

## Hardware Configuration {#hardware}

The required number of servers and disks is determined by fault tolerance requirements. For more details, see [{#T}](../../concepts/topology.md).

* **Processor (CPU)**

  {{ ydb-short-name }} server can only run on x86-64 architecture processors with AVX2 instruction set support: Intel Haswell (4th generation) and later, AMD EPYC and later.

  ARM architecture is currently not supported.

* **Memory**

  It is recommended to use memory with error correction support (ECC) to protect against hardware failures.

* **Disk Subsystem**

  {{ ydb-short-name }} server can run on servers with any type of disks (HDD/SSD/NVMe). However, SSD/NVMe disks are recommended for better performance.

  {% include [_includes/storage-device-requirements.md](../../_includes/storage-device-requirements.md) %}

  {{ ydb-short-name }} does not use a file system for data storage and works with disks directly via block devices. Therefore, you should not mount a file system or perform other operations on the partition used by {{ ydb-short-name }}. It is also not recommended to share the block device with other processes — this can lead to significant performance degradation.

  {{ ydb-short-name }} operability and performance have not been tested on any types of virtual or network storage devices.

  When planning storage capacity, consider that {{ ydb-short-name }} uses part of the disk space for its internal needs. For example, on a medium-sized cluster of 8 nodes, you can expect consumption of about 100 GB for the entire cluster for the static group. On a large cluster with >1500 nodes — about 200 GB. There are also system logs of 25.6 GB on each PDisk and a system area on each PDisk. Its size depends on the PDisk volume but is at least 0.2 GB.

## Software Configuration {#software}

{{ ydb-short-name }} node can run on servers with Linux operating system with kernel version 4.19 and above and libc version 2.30. For example, Ubuntu 20.04, Debian 11, Fedora 34, or newer versions. {{ ydb-short-name }} uses the [TCMalloc](https://google.github.io/tcmalloc) memory allocator; for its efficient operation, it is recommended to [enable](https://google.github.io/tcmalloc/tuning.html#system-level-optimizations) Transparent Huge Pages and Memory overcommitment.

If the server has more than 32 CPU cores, to improve {{ ydb-short-name }} dynamic node performance, they should be run in separate taskset/cpuset of 10 to 32 cores each. For example, with 128 cores, it is optimal to run 4 dynamic nodes: each in its own taskset of 32 cores.

macOS and Windows operating systems are currently not supported for running the server part of {{ ydb-short-name }}. However, for development and functional testing purposes, it is acceptable to run {{ ydb-short-name }} on them in a [Docker container](../../quickstart.md).