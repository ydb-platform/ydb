## Nodes {#nodes}

One {{ ydb-short-name }} installation consists of a *cluster* that is divided into *nodes*. A node is a single process in the system, usually ydbd. This node is part of a cluster and can exchange data with its other nodes via *Interconnect*. Each *node* has its own ID which is usually named NodeId. NodeID is a 20-bit integer equal to or greater than 1. NodeID 0 is reserved for internal needs and usually indicates the current node or no node.

A number of services are run on each node and implemented via *actors*.

Nodes can be static and dynamic.

A configuration of static nodes, that is, their complete list with the address for connecting via Interconnect specified, is stored in a configuration file and is read once when the process is started. A set of static nodes changes very rarely. This usually happens when expanding clusters or moving nodes from one physical machine to another. To change a set of static nodes, you must apply the updated configuration to **every** node and then perform a rolling restart of the entire cluster.

Dynamic nodes are not known in advance and are added to the system as new processes are started. This may be due, for example, to the creation of new tenants in {{ ydb-short-name }} installations as a database. When a dynamic node is registered, its process first connects to one of the static nodes via gRPC, transmits information about itself through a special service called Node Broker, and receives a NodeID to use to log into the system. The mechanism for assigning nodes is pretty much similar to the DHCP in the context of distributing IP addresses.

