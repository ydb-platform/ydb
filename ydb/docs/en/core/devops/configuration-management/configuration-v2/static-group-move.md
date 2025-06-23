# Static Group Move

{% cut "Article under development" %}

{% include [new](../_includes/configuration-version-note.md) %}

When using Configuration V2, static group management is performed automatically and the Self Heal mechanism will perform reconfiguration when one static group node fails.

When manual static group configuration management is needed, you need to disable automatic static group management, get the current static group configuration, make changes and apply the modified configuration as the target static group configuration. Then you need to remove the target static group configuration from the configuration file and enable automatic static group configuration management.

{% include [warning-configuration-error](../configuration-v1/_includes/warning-configuration-error.md) %}

As an example, consider a {{ ydb-short-name }} cluster where a [static node](../../../devops/configuration-management/configuration-v2/config-settings.md#hosts) is configured and running on the host with `node_id:1`. This node serves part of the static group.

Static group configuration fragment:

```yaml
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
```

To replace `node_id:1`, we use another host with a static node deployed on it with `node_id:10`.

To move part of the static group from host `node_id:1` to `node_id:10`:

1. Disable automatic static group management.
2. Get the current static group configuration.
3. Make changes and apply the modified configuration as the target static group configuration.
    In the configuration file `config.yaml`, change the `node_id` value, replacing the identifier of the host being removed with the identifier of the host being added:

    ```yaml
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
    ```

      Change the `path` and disk `pdisk_category` if they differ on the host with `node_id: 10`.
4. Go to the Embedded UI monitoring page and ensure that the static group VDisk appeared on the target physical disk and is replicating. For more details, see [{#T}](../../../reference/embedded-ui/ydb-monitoring.md#static-group).
5. Remove the target static group configuration from the configuration file and enable automatic static group configuration management.

{% endcut %}