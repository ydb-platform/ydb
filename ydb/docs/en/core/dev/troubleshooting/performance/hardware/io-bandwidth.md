# I/O bandwidth

A high rate of read/write operations can overwhelm the disk subsystem, resulting in increased latencies in data access. When the system cannot read or write data quickly enough, queries that depend on disk access will experience delays.

## Diagnostics

<!-- The include is added to allow partial overrides in overlays  -->
{% include notitle [io-bandwidth](./_includes/io-bandwidth.md) %}

## Recommendations

Add more [storage groups](../../../../concepts/glossary.md#storage-group) to the database.

In case of high microburst values, you can also try to balance the load across storage groups.

