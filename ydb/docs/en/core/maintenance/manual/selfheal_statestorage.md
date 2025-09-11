# Working with SelfHeal State Storage


During cluster operation, the entire nodes on which {{ydb-short-name }} is running may fail.

Self Heal State Storage is used to maintain the operability and fault tolerance of the [StateStorage] subsystem(../../../concepts/glossary.md#state-storage), [Board](../../../concepts/glossary.md#board), [SchemeBoard](../../../concepts/glossary.md#scheme-board) of a cluster, if it is impossible to quickly restore failed nodes, and automatically increase the number of replicas of these subsystems when new nodes are added to the cluster.

Self Heal State Storage allows you to:

* detect faulty system components;
* move replicas of [StateStorage](../../../concepts/glossary.md#state-storage), [Board](../../../concepts/glossary.md#board), [SchemeBoard](../../../concepts/glossary.md#scheme-board) to other nodes or add new replicas.

Self Heal State Storage is enabled by default.

The {{ydb-short-name }} component responsible for Self Heal State Storage is called [CMS Sentinel](../../../concepts/glossary.md#cms).

## Turning Self Heal State Storage on and off {#on-off}

You can turn Self Heal State Storage on and off by changing the configuration.
The mechanism requires activation of both [CMS Sentinel](../../../concepts/glossary.md#cms) and [distributed configuration](../../../concepts/glossary.md#distributed-configuration).

1. Get the current cluster configuration using the command [ydb admin cluster config fetch](../../../reference/ydb-cli/commands/configuration/cluster/fetch.md):

    ```bash
    ydb -e grpc://<node.ydb.tech>:2135 admin cluster config fetch > config.yaml
    ```
2. Change the configuration file `config.yaml` by changing the value of the parameter `state_storage_self_heal_config.enable` to `true` or to `false`:

    ```yaml
    config:
        cms_config:
            sentinel_config:
                state_storage_self_heal_config:
                    enable: true # Включение self heal state storage
    ```
    {% cut "More detailed" %}

    The mechanism requires activation of both [CMS Sentinel](../../../concepts/glossary.md#cms) and [distributed configuration](../../../concepts/glossary.md#distributed-configuration). Make sure they are enabled.

    The `state_storage_self_heal_config` option is responsible for managing the mechanism for maintaining health and fault tolerance [StateStorage](../../../concepts/glossary.md#state-storage), [Board](../../../concepts/glossary.md#board), [SchemeBoard](../../../concepts/glossary.md#scheme-board)

    {% endcut %}

3. Upload the updated configuration file to the cluster using [ydb admin cluster config replace](../../../reference/ydb-cli/commands/configuration/cluster/replace.md):

    ```bash
    ydb -e grpc://<node.ydb.tech>:2135 admin cluster config replace -f config.yaml
    ```
