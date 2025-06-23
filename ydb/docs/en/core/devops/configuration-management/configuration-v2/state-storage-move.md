# State Storage Move

{% cut "Article under development" %}

{% include [new](../_includes/configuration-version-note.md) %}

If you need to decommission a {{ ydb-short-name }} cluster node that contains part of [State Storage](../../../reference/configuration/index.md#domains-state), you need to move it to another node.

{% include [warning-configuration-error](../configuration-v1/_includes/warning-configuration-error.md) %}

When using Configuration V2, State Storage configuration is performed automatically. To make changes to the configuration, you need to perform the following actions: stop automatic State Storage configuration management, get the current State Storage configuration, change the current configuration in the desired way and specify it explicitly as the target, then apply it, ensure it is applied, perform node decommissioning, remove explicit State Storage configuration specification and enable automatic State Storage configuration.

As an example, consider a {{ ydb-short-name }} cluster where node 1 is part of State Storage, and node 10 is not, and we will assume that the goal of changing the State Storage configuration is to decommission node 1.

1. Stop automatic State Storage configuration management
2. Get the current State Storage configuration
3. Change the current configuration in the desired way and specify it explicitly as the target

    Suppose the following State Storage configuration is obtained:

    ```yaml
    ...
      state_storage:
      - ring:
          node: [1, 2, 3, 4, 5, 6, 7, 8, 9]
          nto_select: 9
        ssid: 1
    ...
    ```

    A [static node](../../../devops/configuration-management/configuration-v2/config-settings.md#hosts) is configured and running on the host with `node_id:1`, which is a part of State Storage. Suppose we need to decommission this host.

    To replace `node_id:1`, we use another host with a static node deployed on it with `node_id:10`.

    To move State Storage from node `node_id:1` to `node_id:10`, in the configuration file `config.yaml` change the `node` host list, replacing the identifier of the node to be removed with the identifier of the node to be added:

    ```yaml
    ...
      state_storage:
      - ring:
          node: [10, 2, 3, 4, 5, 6, 7, 8, 9]
          nto_select: 9
        ssid: 1
    ...
    ```

4. Apply the configuration.
5. Ensure the configuration is applied.
   Note that the full application of new State Storage nodes after reconfiguration occurs with a delay of at least 15 seconds.
6. Perform node decommissioning.
7. Remove explicit State Storage configuration specification.
8. Enable automatic State Storage configuration.

{% endcut %}