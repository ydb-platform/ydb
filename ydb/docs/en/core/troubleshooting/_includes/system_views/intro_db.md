# Database system views

To enable internal introspection of the DB state, the user can make queries to special service tables (system views). These tables are accessible from the root of the database tree and use the `.sys` system path prefix.

Hereinafter, in the descriptions of available fields, the **Key** column contains the corresponding table's primary key field index.

System views contain the following information:

* [Details of individual DB table partitions](#partitions).
* [Top queries by certain characteristics](#top-queries).
* [Query details](#query-metrics).
* [History of overloaded partitions](#top-overload-partitions).
