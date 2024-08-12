# Moving a State Storage

To decommission a {{ ydb-short-name }} cluster host that accommodates a part of a [State Storage](../../deploy/configuration/config.md#domains-state), you need to move the group to another host.

{% include [warning-configuration-error](../_includes/warning-configuration-error.md) %}

As an example, let's take a {{ ydb-short-name }} cluster with the following State Storage configuration:

```yaml
...
domains_config:
  ...
  state_storage:
  - ring:
      node: [1, 2, 3, 4, 5, 6, 7, 8, 9]
      nto_select: 9
    ssid: 1
  ...
...
```

The [static node](../../deploy/configuration/config.md#hosts) of the cluster that serves a part of State Storage is set up and running on the host with `node_id:1`. Suppose that you want to decommission this host.

To replace `node_id:1`, we [added](../../maintenance/manual/cluster_expansion.md#add-host) to the cluster a new host with `node_id:10` and [deployed](../../maintenance/manual/cluster_expansion.md#add-static-node) a static node in it.

To move State Storage from the `node_id:1` host to the `node_id:10` host:

1. Stop the cluster's static nodes on the hosts with `node_id:1` and `node_id:10`.

   {% include [fault-tolerance](../_includes/fault-tolerance.md) %}
1. In the `config.yaml` configuration file, change the `node` host list, replacing the ID of the removed host by the ID of the added host:

   ```yaml
   domains_config:
   ...
     state_storage:
     - ring:
         node: [2, 3, 4, 5, 6, 7, 8, 9, 10]
         nto_select: 9
       ssid: 1
   ...
   ```

1. Update the `config.yaml` configuration files for all the cluster nodes, including dynamic nodes.
1. Use the [rolling-restart](../../maintenance/manual/node_restarting.md) procedure to restart all the cluster nodes (including dynamic nodes but excluding static nodes on the hosts with `node_id:1` and  `node_id:10`).
1. Stop static cluster nodes on the hosts with `node_id:1` and `node_id:10`.
