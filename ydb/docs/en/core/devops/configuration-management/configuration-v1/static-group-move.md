# Static Group Move

{% include [deprecated](_includes/deprecated.md) %}

If you need to decommission a {{ ydb-short-name }} cluster host that contains part of the [static group](../../../reference/configuration/index.md#blob_storage_config), you need to move it to another host.

{% include [warning-configuration-error](_includes/warning-configuration-error.md) %}

As an example, consider a {{ ydb-short-name }} cluster where a [static node](../../../reference/configuration/index.md#hosts) is configured and running on the host with `node_id:1`. This node serves part of the static group.

Static group configuration fragment:

```yaml
...
blob_storage_config:
  ...
  service_set:
    ...
    groups:
      ...
      rings:
        ...
        fail_domains:
        - vdisk_locations:
          - node_id: 1
            path: /dev/vda
            pdisk_category: SSD
        ...
      ...
    ...
  ...
...
```

To replace `node_id:1`, we [added](cluster-expansion.md#add-static-node) a new host with `node_id:10` to the cluster and [deployed](cluster-expansion.md#add-static-node) a static node on it.

To move part of the static group from host `node_id:1` to `node_id:10`:

1. Stop the cluster static node on the host with `node_id:1`.

    {% include [fault-tolerance](_includes/fault-tolerance.md) %}

2. In the configuration file `config.yaml`, change the `node_id` value, replacing the identifier of the host being removed with the identifier of the host being added:

    ```yaml
    ...
    blob_storage_config:
      ...
      service_set:
        ...
        groups:
          ...
          rings:
            ...
            fail_domains:
            - vdisk_locations:
              - node_id: 10
                path: /dev/vda
                pdisk_category: SSD
            ...
          ...
        ...
      ...
    ...
    ```

    Change the `path` and disk `pdisk_category` if they differ on the host with `node_id: 10`.

3. Update the configuration files `config.yaml` for all cluster nodes, including dynamic ones.
4. Using the [rolling-restart](../../../maintenance/manual/node_restarting.md) procedure, restart all static cluster nodes.
5. Go to the Embedded UI monitoring page and ensure that the static group VDisk appeared on the target physical disk and is replicating. For more details, see [{#T}](../../../reference/embedded-ui/ydb-monitoring.md#static-group).
6. Using the [rolling-restart](../../../maintenance/manual/node_restarting.md) procedure, restart all dynamic cluster nodes.