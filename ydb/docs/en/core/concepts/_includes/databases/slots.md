## Slots {#slots}

_A slot_ is the portion of a server's resources allocated to running a single {{ ydb-short-name }} cluster node. It is constant and equal to 10 CPUs/50 GB RAM. Slots are used if a {{ ydb-short-name }} cluster is deployed on bare-metal instances whose resources are sufficient to host multiple slots. If you use VMs for cluster deployment, their capacity is selected so that the use of slots is not required: one node serves one database and one database can use multiple nodes allocated to it.

You can see a list of DB slots in the output of the `discovery list` command of the {{ ydb-short-name }} console client.

