## {{ ydb-short-name }} architecture in different modes of operation {#arch-diff}

### Separate compute and storage layers {#separate-layers}

Please note that {{ ydb-short-name }} has two separate explicit layers: storage and compute. The storage layer is responsible for securely storing data on storage devices and replicating data between nodes to ensure fault tolerance.

In {{ ydb-short-name }}, user data is stored in tables that are partitioned. A shard is an entity that is responsible for storing a table partition (typically one). An entity called a tablet is responsible for changing data in a shard. It's a component that implements consistent changes in the shard data and solves the issue of distributed consensus. A tablet instance can be viewed as an object that is generated in the process address space and consumes CPU resources and RAM. Tablets store all their statuses in the storage layer. This means, among other things, that a tablet instance can be raised in an arbitrary process that the storage layer is available from. The {{ ydb-short-name }} compute layer essentially consists of tablets and the YQL query execution layer.

It should be noted that the concept of a database comprises user tables and, accordingly, tablet instances serving these tables as well as certain system entities. For example, there is a tablet called SchemeShard. It serves the data schema of all tables. There is a coordination system for distributed transactions whose items are also tablets.

### {{ ydb-short-name }} Dedicated mode {#dedicated}

Dedicated mode assumes that resources for tablet instances and for executing YQL queries are selected from the compute resources explicitly allocated to the database. Computational resources are VMs that have a certain number of vCPUs and some memory. The task of selecting the optimal amount of resources for the DB is currently the user's responsibility. If there aren't enough resources to serve the load, the latency of requests increases, which may eventually lead to the denial of service for requests, such as that with the `OVERLOADED` return code. The user can add compute resources (VMs) to the database in the UI or CLI to ensure it has the necessary amount of computing resources. Adding compute resources to the DB is relatively fast and comparable to the time it takes to start a VM. Subsequently, {{ ydb-short-name }} automatically balances tablets across a cluster account taken of resources added.

### {{ ydb-short-name }} Serverless mode {#serverless}

In Serverless mode, the {{ ydb-short-name }} infrastructure determines the amount of computational resources to allocate to support a user database. The amount of allocated resources can be either very large (an arbitrary number of cores) or very small (significantly less than one core). If a user created a DB with a single table with a single entry and only rarely makes DB queries, {{ ydb-short-name }} actually uses a small amount of RAM on tablet instances that are part of the user DB. This is possible due to the fact that the user database components are objects rather than processes. If the load increases, the DB components start using more CPU time and memory. If load grows to the point where there aren't enough VM resources, the {{ ydb-short-name }} infrastructure can balance the system granularly spawning tablet instances on other VMs.

This technology lets you package virtual entities (tablet instances) very tightly together into physical resources based on actual consumption. This makes it possible to invoice the user for the operations performed rather than the allocated resources.

