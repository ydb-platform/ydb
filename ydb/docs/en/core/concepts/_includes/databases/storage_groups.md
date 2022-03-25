## Storage groups {#storage-groups}

A storage group is a redundant array of independent disks that are networked in a single logical unit. Storage groups increase fault tolerance through redundancy and improve performance.

Each storage group corresponds to a specific storage schema that affects the number of disks used, the failure model, and the redundancy factor. The `block4-2` arrangement is commonly used for single-datacenter clusters with storage located on 8 disks in 8 racks and capable of surviving a failure of any two disks, and ensuring a redundancy factor of 1.5. Multiple-datacenter clusters use the `mirror3dc` arrangement with storage comprising 9 disks, 3 in each of the three datacenters, and capable of surviving a failure of a datacenter or disk and ensuring a redundancy factor of 3.

{{ ydb-short-name }} lets you allocate additional storage groups for available DBs as your data grows.

