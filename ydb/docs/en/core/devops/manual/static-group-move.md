# Moving a static group

To decommission a {{ ydb-short-name }} cluster host that accommodates a part of a [static group](../../reference/configuration/index.md#blob_storage_config), you need to move the group to another host.

{% include [warning-configuration-error](../_includes/warning-configuration-error.md) %}

As an example, let's take a {{ ydb-short-name }} cluster where you set up and launched a [static node](../../reference/configuration/index.md#hosts) on a host with `node_id:1`. This node serves a part of the static group.

Fragment of the static group configuration:

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

To replace `node_id:1`, we [added](../../maintenance/manual/cluster_expansion.md#add-host) to the cluster a new host with `node_id:10` and [deployed](../../maintenance/manual/cluster_expansion.md#add-static-node) a static node in it.

To move a part of the static group from the `node_id:1` host to the `node_id:10` host:

1. Stop the static cluster node on the host with `node_id:1`.

   {% include [fault-tolerance](../_includes/fault-tolerance.md) %}

2. In the `config.yaml` configuration file, change `node_id`, replacing the ID of the removed host by the ID of the added host:

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

   Edit the `path` and `pdisk_category` for the disk if these parameters are different on the host with `node_id: 10`.

3. Update the `config.yaml` configuration files for all the cluster nodes, including dynamic nodes.
4. Use the [rolling-restart](../../maintenance/manual/node_restarting.md) procedure to restart all the static cluster nodes.
5. Go to the Embedded UI monitoring page and make sure that the VDisk of the static group is visible on the target physical disk and its replication is in progress. For details, see [{#T}](../../reference/embedded-ui/ydb-monitoring.md#static-group).
6. Use the [rolling-restart](../../maintenance/manual/node_restarting.md) procedure to restart all the dynamic cluster nodes.
