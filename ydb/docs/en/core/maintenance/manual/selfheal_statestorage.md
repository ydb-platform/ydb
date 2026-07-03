# Self Heal State Storage

{% note warning %}

This guide applies only to {{ ydb-short-name }} clusters with **configuration V2** and **distributed configuration**. On clusters with **configuration V1**, these steps and commands (including fetching configuration with `ydb admin cluster config fetch`) are unavailable or will not produce the expected result. No alternatives for V1 are described here — see [Migration to V2 configuration](../../devops/configuration-management/migration/migration-to-v2.md).

{% endnote %}

During cluster operation, entire nodes on which {{ ydb-short-name }} is running may fail.

Self Heal State Storage keeps the [metadata distribution subsystem](../../concepts/glossary.md#state-storage), [Board](../../concepts/glossary.md#board), and [SchemeBoard](../../concepts/glossary.md#scheme-board) operational and fault tolerant when failed nodes cannot be restored quickly, and automatically increases the number of replicas of these subsystems when new nodes are added to the cluster.

Self Heal State Storage:

* detects faulty {{ ydb-short-name }} cluster nodes;
* moves replicas of [StateStorage](../../concepts/glossary.md#state-storage), [Board](../../concepts/glossary.md#board), and [SchemeBoard](../../concepts/glossary.md#scheme-board) to other nodes or adds new replicas.

The Self Heal State Storage component is part of the cluster management system [CMS Sentinel](../../concepts/glossary.md#cms).

## Turning Self Heal State Storage on and off {#on-off}

You can turn Self Heal State Storage on or off by changing the configuration.
The mechanism requires both [CMS Sentinel](../../concepts/glossary.md#cms) and [distributed configuration](../../concepts/glossary.md#distributed-configuration) to be enabled.

1. Fetch the current cluster configuration using the [ydb admin cluster config fetch](../../reference/ydb-cli/commands/configuration/cluster/fetch.md) command:

    ```bash
    ydb [global options...] admin cluster config fetch > config.yaml
    ```

2. Edit the `config.yaml` file by setting the `state_storage_self_heal_config.enable` parameter to `true` or `false`:

    ```yaml
    config:
        self_management_config:
            enabled: true # Enable distributed configuration
        cms_config:
            sentinel_config:
                enable: true # Enable Sentinel
                state_storage_self_heal_config:
                    enable: true # Enable self heal state storage
    ```

    The mechanism requires both [CMS Sentinel](../../concepts/glossary.md#cms) and [distributed configuration](../../concepts/glossary.md#distributed-configuration). Make sure they are enabled.
    Learn more in [Migration to V2 configuration and enabling distributed configuration](../../devops/configuration-management/migration/migration-to-v2.md).
    Setting `state_storage_self_heal_config.enable` to `true` enables the mechanism that maintains health and fault tolerance of [StateStorage](../../concepts/glossary.md#state-storage), [Board](../../concepts/glossary.md#board), and [SchemeBoard](../../concepts/glossary.md#scheme-board).

3. Apply the updated configuration to the cluster using [ydb admin cluster config replace](../../reference/ydb-cli/commands/configuration/cluster/replace.md):

    ```bash
    ydb [global options...] admin cluster config replace -f config.yaml
    ```

## How to verify the result {#verify-result}

To verify that the changes took effect, open the `CMS` section in the cluster [Embedded UI](../../reference/embedded-ui/index.md) (available on port 8765) and go to the `Sentinel` tab to view the Sentinel and Self Heal State Storage status.
