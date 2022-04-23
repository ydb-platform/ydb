# Cluster system tables

To enable internal introspection of the cluster state, the user can make queries to special service tables (system views). These tables are accessible from the cluster's root directory and use the `.sys` system path prefix.

Usually, cloud database users have no access to cluster system tables, as cluster support and timely diagnostics are the prerogative of the cloud team.

Hereinafter, in the descriptions of available fields, the **Key** column contains the corresponding table's primary key field index.

