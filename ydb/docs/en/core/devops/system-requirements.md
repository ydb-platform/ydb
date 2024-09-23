# {{ ydb-short-name }} system requirements and recommendations

This section provides recommendations for deploying {{ ydb-short-name }} clusters that are relevant regardless of the chosen deployment method ([Ansible](./ansible/index.md), [Kubernetes](./kubernetes/index.md), or [manual](./manual/index.md)).

## Hardware configuration {#hardware}

The fault-tolerance requirements determine the necessary number of servers and disks. For more information, see [{#T}](../concepts/topology.md).

* **Processor (CPU)**

 A {{ ydb-short-name }} server can only run on x86-64 processors with AVX2 instruction support: Intel Haswell (4th generation) and later, AMD EPYC and later.

 The ARM architecture is currently not supported.

* **RAM**

 We recommend using error-correcting code (ECC) memory to protect against hardware failures.

* **Disk subsystem**

 A {{ ydb-short-name }} server can run on servers with any disk type (HDD/SSD/NVMe). However, we recommend using SSD/NVMe disks for better performance.

 {% include [_includes/storage-device-requirements.md](../_includes/storage-device-requirements.md) %}

 {{ ydb-short-name }} works with disk drives directly and does not use any filesystem to store data. Don't mount a file system or perform other operations with partitions used by {{ ydb-short-name }}. Also, avoid sharing the {{ ydb-short-name }}'s block device with the operating system and different processes, which can lead to significant performance degradation.

 Prefer to use physical local disk drives for {{ ydb-short-name }} instead of virtual or network storage devices.

 Remember that {{ ydb-short-name }} uses some disk space for internal needs when planning disk capacity. For example, on a medium-sized cluster of 8 nodes, you can expect approximately 100 GB to be consumed for a static group on the whole cluster. On a large cluster with more than 1500 nodes, this will be about 200 GB. There are also 25.6 GB of logs on each Pdisk and a system area on each Pdisk. Its size depends on the size of the Pdisk but is no less than 0.2 GB.

## Software configuration {#software}

A {{ ydb-short-name }} server can be run on servers with a Linux operating system, kernel 4.19 and higher, and libc 2.30. For example, Ubuntu 20.04, Debian 11, Fedora 34, or newer releases. {{ ydb-short-name }} uses the [TCMalloc](https://google.github.io/tcmalloc) memory allocator. To make it efficient, [enable](https://google.github.io/tcmalloc/tuning.html#system-level-optimizations) Transparent Huge Pages and Memory overcommitment.

If the server has more than 32 CPU cores, to increase {{ ydb-short-name }} performance, run each dynamic node in a separate taskset/cpuset of 10 to 32 cores. For example, in the case of 128 CPU cores a viable approach would be to run four 32-CPU dynamic nodes, each in a dedicated taskset.

MacOS and Windows operating systems are currently unsupported for running production {{ ydb-short-name }} servers. However, running {{ ydb-short-name }} in a [Docker container](../quickstart.md) on them is acceptable for development and functional testing.