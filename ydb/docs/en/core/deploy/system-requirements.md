# System requirements and recommendations

This section provides recommendations for deploying {{ ydb-short-name }}.

## Hardware configuration {#hardware}

* **CPU**

  {{ ydb-short-name }} server can be run only on x86-64 CPU with AVX2 instructions support (Intel Haswell (4th generation) and later, AMD EPYC and later). 
  ARM processors are not supported now.

* **Memory**

  It is recommended to use error-correcting code (ECC) memory for data consistency.

* **Storage**

  {{ ydb-short-name }} server can be run on servers with any storage types (HDD/SSD/NVMe/RAM). It is recommended to use SSD/NVMe drives for better performance.

  Minimal disk capacity must be at least 80GB. It is recommended to use such disks only for testing, proper functioning is not guaranteed. It is recommended to use disks as block devices with more than 800GB capacity for efficiency.

  {{ ydb-short-name }} does not use any filesystems to store data and works  directly with disk. Don't create or mount filesystems on disks which are used in {{ ydb-short-name }} . Also it is not recommended to share block devices with other processes as this can lead to a significant performance degradation. 


## Software configuration {#software}

{{ ydb-short-name }} server can be run on OS Linux with kernel 4.4 or later. MacOS and Windows are not supported now.

## Topology {#topology}

When planning a deployment, it's important to choose the right [Blob Storage configuration](./configuration/config.md#domains-blob) based on the required fault tolerance:

 * Mode None - This mode requires 1 server without fault tolerance. It is recommended only for functional testing.

 * block-4-2: This mode requires at least 8 servers in one datacenter. To ensure maximum fault tolerance, servers must be located in 8 independent racks. In this mode cluster remains operational if 1 rack fails completely and 1 node in another rack fails.

 * Mode mirror-3dc - This mode requires at least 9 servers. To ensure maximum fault tolerance, servers must be located in three independent data centers and different server racks in each of them. In this mode cluster remains operational if 1 datacenter fails completely and 1 node fails in another datacenter.

  * Mode mirror-3dc - This mode requires at least 9 servers. To ensure maximum fault tolerance, servers must be located in three independent data centers and different server racks in each of them. In this mode cluster remains operational if 1 datacenter fails completely and 1 node fails in another datacenter.

  * Mode mirror-3dc-3-nodes - The simple variant of mirror-3dc mode. This mode requires at least 3 servers with 3 disks in each. To ensure maximum fault tolerance, each server must be located in an independent datacenter. In this mode cluster keeps running if no more than 1 node fails. It is recommended only for testing.

The failure of the node means that the server is out of service completely or one of the disks in it fails.

Read more about storage groups in [Terms and definitions](../concepts/databases.md#storage-groups).