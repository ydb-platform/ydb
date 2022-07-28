# System requirements and recommendations

This section provides recommendations for deploying {{ ydb-short-name }}.

## Hardware configuration {#hardware}

The number of servers and disks is determined by the fault-tolerance requirements. For more information, see [{#T}](topology.md).

* **Processor**

   A {{ ydb-short-name }} server can only run on x86-64 processors with AVX2 instruction support (Intel Haswell (4th generation) and later, AMD EPYC and later).

   The ARM architecture is currently not supported.

* **RAM**

   We recommend using error-correcting code (ECC) memory to protect against hardware failures.

* **Disk subsystem**

   A {{ ydb-short-name }} server can run on servers with any disk type (HDD/SSD/NVMe). However, we recommend using SSD/NVMe disks for better performance.

   The minimum disk size must be at least 80 GB, otherwise the node won't be able to use the block store volume. Correct and uninterrupted operation with minimum-size disks is not guaranteed. We recommend using such disks exclusively for testing purposes. For YDB to work efficiently, we recommend using disks larger than 800 GB as block devices.

   {{ ydb-short-name }} does not use a file system to store data and accesses disk volumes directly. Don't mount a file system or perform other operations with a partition that uses {{ ydb-short-name }}. We also do not recommend sharing the block device with other processes because this can lead to significant performance degradation.

## Software configuration {#software}

A {{ ydb-short-name }} server can run on servers with Linux kernel 4.4 or higher.

MacOS and Windows operating systems are currently not supported.
