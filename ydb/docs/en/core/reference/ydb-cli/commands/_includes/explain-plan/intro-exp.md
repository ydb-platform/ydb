The main section of the query plan, `tables`, contains information about querying tables. Reads are described in the `reads` section and writes in the `writes` section. The key characteristic of any table query is its type.

Types of reads:

* `FullScan`: Full table scan. All entries on all shards are read.
* `Scan`: A read of a certain range of entries.
* `Lookup`: A read by key or key prefix.
* `MultiLookup`: Multiple reads by key or key prefix. Supported, for example, in JOINs.

Types of writes:

* `Upsert`: Add a single entry.
* `MultiUpsert`: Add multiple entries.
* `Erase`: A single delete by key.
* `MultiErase`: Multiple deletes.

Let's take the query plan from the example above.
The `lookup_by` parameter shows what columns (key or key prefix) reads are made by.
The `scan_by` parameter shows what columns a read of all entries in a certain range of values is made by.
The `columns` parameter lists the columns whose values will be read from the table.

