## Checklist for deploying your product environment

This section provides recommendations for deploying {{ ydb-full-name }} in product environments.

## Topology

When planning a deployment, it's important to choose the right cluster topology based on the required fault tolerance:

* Mirror-3dc-3-nodes: This mode requires at least 3 servers with 3 disks in each. To ensure maximum fault tolerance, each server must be located in an independent datacenter. In this mode, a cluster keeps running if no more than 1 server fails.
* Mirror-3dc: This mode requires at least 9 servers. To ensure maximum fault tolerance, servers must be located in three independent data centers and different server racks in each of them. In this mode, a cluster keeps running if 1 datacenter fails completely and 1 server in another datacenter is out of service.
* Block-4-2: This mode requires at least 8 servers in one datacenter. To ensure maximum fault tolerance, servers must be located in 8 independent racks.

TBD.  mirror-3dc vs block-4-2: Whether we want the following in the doc:

- Much more network traffic
- Higher storage overhead

## Hardware configuration

For correct operation, we recommend using the x86_64 CPU architecture with support for the following instructions:

- SSE
- ???

## Software configuration

