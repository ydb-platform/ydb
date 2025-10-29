# State Storage Move

{% include [deprecated](_includes/deprecated.md) %}

If you need to decommission a {{ ydb-short-name }} cluster host that contains part of [State Storage](../../../reference/configuration/domains_config.md#domains-state), you need to move it to another host.

{% include [warning-configuration-error](_includes/warning-configuration-error.md) %}

As an example, consider a {{ ydb-short-name }} cluster with the following State Storage configuration:

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

On the host with `node_id:1`, a cluster [static node](../../../reference/configuration/hosts.md#hosts) is configured and running, which serves part of State Storage. Suppose we need to decommission this host.

To replace `node_id:1`, we [added](cluster-expansion.md#add-host) a new host with `node_id:10` to the cluster and [deployed](cluster-expansion.md#add-static-node) a static node on it.

To move State Storage from host `node_id:1` to `node_id:10`:

1. Stop the cluster static nodes on hosts with `node_id:1` and `node_id:10`.

    {% include [fault-tolerance](_includes/fault-tolerance.md) %}

1. In the configuration file `config.yaml`, change the `node` host list, replacing the identifier of the host being removed with the identifier of the host being added:

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

1. Update the configuration files `config.yaml` for all cluster nodes, including dynamic ones.
1. Using the [rolling-restart](../../../maintenance/manual/node_restarting.md) procedure, restart all cluster nodes, including dynamic ones, except for static nodes on hosts with `node_id:1` and `node_id:10`. Note that a delay of at least 15 seconds is required between host restarts.
1. Start the cluster static nodes on hosts `node_id:1` and `node_id:10`.