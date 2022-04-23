# Database system tables

To enable internal introspection of the DB state, the user can make queries to special service tables (system views). These tables are accessible from the root of the database tree and use the `.sys` system path prefix.

Hereinafter, in the descriptions of available fields, the **Key** column contains the corresponding table's primary key field index.

The article describes the following system tables:

* [Partitions](#partitions)
* [Top queries](#top-queries)
* [Query details](#query-metrics)
