# Maintaining a cluster's disk subsystem

## Tackling cluster failures

The cluster may become inoperable for a number of reasons:

* It may [go beyond the failure model](failure_model.md), stopping data reads and writes to the storage group completely.

* Unbalanced workload on disks may strongly affect the request processing latency. Load balancing methods are described in this [article](balancing_load.md).

* Writing can also stop if multiple physical disks run out of space. This can be solved [by freeing up space](disk_end_space.md) or adding block store volumes to [expand the cluster](cluster_expansion.md).

Unauthorized withdrawal of nodes can result in issues described above. To prevent the issues, make sure to [drain the nodes correctly for maintenance](node_restarting.md).

Enabling [Scrubbing](scrubbing.md) and [SelfHeal](selfheal.md) would also be a good preventative measure.

## Editing the cluster configuration

A {{ ydb-short-name }} cluster lets you:

* [Expand](cluster_expansion.md) block store volumes and nodes.
* [Configure](change_actorsystem_configs.md) the actor system on your nodes.
* Edit configs via [CMS](cms.md).
* [Add](adding_storage_groups.md) new storage groups.

