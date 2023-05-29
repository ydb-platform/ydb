# System requirements and recommendations

This section provides recommendations for deploying {{ ydb-short-name }}.

## Hardware configuration {#hardware}

The number of servers and disks is determined by the fault-tolerance requirements. For more information, see [{#T}](topology.md).

* **Processor**

   A {{ ydb-short-name }} server can only run on x86-64 processors with AVX2 instruction support: Intel Haswell (4th generation) and later, AMD EPYC and later.

   The ARM architecture is currently not supported.

* **RAM**

   We recommend using error-correcting code (ECC) memory to protect against hardware failures.

* **Disk subsystem**

   A {{ ydb-short-name }} server can run on servers with any disk type (HDD/SSD/NVMe). However, we recommend using SSD/NVMe disks for better performance.

   {% include [_includes/storage-device-requirements.md](../_includes/storage-device-requirements.md) %}

   {{ ydb-short-name }} does not use a file system to store data and accesses disk volumes directly. Don't mount a file system or perform other operations with a partition that uses {{ ydb-short-name }}. We also do not recommend sharing the block device with other processes because this can lead to significant performance degradation.

   {{ ydb-short-name }} health and performance weren't tested on any types of virtual or network storage devices.

   When planning space, remember that {{ ydb-short-name }} uses some disk space for its own internal needs. For example, on a medium-sized cluster of 8 nodes, you can expect approximately 100 GB to be consumed for a static group on the whole cluster. On a large cluster with more than 1500 nodes, this will be about 200 GB. There are also logs of 25.6 GB on each Pdisk and a system area on each Pdisk. Its size depends on the size of the Pdisk, but is no less than 0.2 GB.

## Software configuration {#software}

A {{ ydb-short-name }} server can be run on servers running a Linux operating system with kernel 4.19 and higher and libc 2.30 (Ubuntu 20.04, Debian 11, Fedora34). YDB uses the [TCMalloc](https://google.github.io/tcmalloc) memory allocator. To make it effective, [enable](https://google.github.io/tcmalloc/tuning.html#system-level-optimizations) Transparent Huge Pages and Memory overcommitment.

If the server has more than 32 CPU cores, to increase YDB performance, it makes sense to run each dynamic node in a separate taskset/cpuset of 10 to 32 cores. For example, in the case of 128 CPU cores, the best choice is to run four 32-CPU dynamic nodes, each in its taskset.

MacOS and Windows operating systems are currently not supported for running {{ ydb-short-name }} servers.
