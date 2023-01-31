# Database system views

You can make queries to special service tables (system views) to monitor the DB status. These tables are accessible from the root of the database tree and use the `.sys` system path prefix.

You can find the corresponding table's primary key field index in the descriptions of available fields below.

DB system views contain:

* [Details of individual DB table partitions](#partitions).
* [Top queries by certain characteristics](#top-queries).
* [Query details](#query-metrics).
* [History of overloaded partitions](#top-overload-partitions).

{% include [notes](notes.md) %}
