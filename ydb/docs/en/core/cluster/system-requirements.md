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

   {% include [_includes/storage-device-requirements.md](../_includes/storage-device-requirements.md) %}

   {{ ydb-short-name }} does not use a file system to store data and accesses disk volumes directly. Don't mount a file system or perform other operations with a partition that uses {{ ydb-short-name }}. We also do not recommend sharing the block device with other processes because this can lead to significant performance degradation.

   {{ ydb-short-name }} health and performance weren't tested on any types of virtual or network storage devices.

## Software configuration {#software}

A {{ ydb-short-name }} server can run on servers with Linux kernel 4.4 or higher.

MacOS and Windows operating systems are currently not supported.
