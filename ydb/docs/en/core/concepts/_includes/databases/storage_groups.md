## Storage groups {#storage-groups}

A storage group is a redundant array of independent disks that are networked in a single logical unit. Storage groups increase fault tolerance through redundancy and improve performance.

Each storage group corresponds to a specific storage schema that affects the number of disks used, the failure model, and the redundancy factor. The ``block4-2`` schema is commonly used for single data center clusters, where the storage group is located on 8 disks in 8 racks, can withstand the failure of any two disks, and ensures a redundancy factor of 1.5. In multiple data center clusters, we use the ``mirror3dc`` schema, where storage groups are made up of 9 disks, 3 in each of the three data centers, which can survive the failure of a data center or disk, and ensures a redundancy factor of 3.

{{ ydb-short-name }} lets you allocate additional storage groups for available DBs as your data grows.
