# I/O bandwidth

A high rate of read and write operations can overwhelm the disk subsystem, leading to increased data access latencies. When the system cannot read or write data quickly enough, queries that rely on disk access will experience delays.

## Diagnostics

<!-- The include is added to allow partial overrides in overlays  -->
{% include notitle [io-bandwidth](./_includes/io-bandwidth.md) %}

## Recommendations

Add more [storage groups](../../../concepts/glossary.md#storage-group) to the database.

In cases of high microburst rates, balancing the load across storage groups might help.

