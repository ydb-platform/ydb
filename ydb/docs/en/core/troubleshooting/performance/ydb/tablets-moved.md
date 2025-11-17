# Frequent tablet moves between nodes

{{ ydb-short-name }} automatically balances the load by moving tablets from overloaded nodes to other nodes. This process is managed by [Hive](../../../concepts/glossary.md#hive). When Hive moves tablets, queries affecting those tablets might experience increased latencies while they wait for the tablet to get initialized on the new node.

{{ ydb-short-name }} considers usage of the following hardware resources for balancing nodes:

* CPU
* Memory
* Network
* [Counter](*count)

Autobalancing occurs in the following cases:

*   **Imbalanced Hardware Resource Usage**

    {{ ydb-short-name }} uses the Scatter metric to evaluate the balance of hardware resource usage. For more details on the Scatter metric's calculation logic and balancing triggers, see the [{#T}](../../../contributor/hive.md#scatter) section.

* **Overloaded nodes (CPU and memory usage)**

    Hive initiates balancing in case of a significant load asymmetry (for example, > 90% on one node and < 70% on another). Learn more here: [{#T}](../../../contributor/hive.md#emergency).

* **Uneven distribution of database objects**

    For tablets with no explicit resource consumption, Hive uses a fake **Counter** resource to ensure their even distribution. Balancing is triggered if this distribution becomes skewed. Learn more: [{#T}](../../../contributor/hive.md#imbalance).


## Diagnostics

<!-- The include is added to allow partial overrides in overlays  -->
{% include notitle [#](_includes/tablets-moved.md) %}

## Recommendations

Adjust Hive balancer settings:

1. Open [Embedded UI](../../../reference/embedded-ui/index.md).

1. Click **Developer UI** in the upper right corner of the Embedded UI.

1. In the **Developer UI**, navigate to **Tablets > Hive > App**.

    ![](_assets/hive-app.png)

1. Click **Settings**.

1. To reduce the likelihood of overly frequent balancing, increase the following Hive balancer thresholds:

    #|
    || Parameter | Description | Default value ||
    || MinCounterScatterToBalance
    | The threshold for the counter scatter value. When this value is reached, Hive starts balancing the load.
    | 0.02 ||
    || MinCPUScatterToBalance
    | The threshold for the CPU scatter value. When this value is reached, Hive starts balancing the load.
    | 0.5 ||
    || MinMemoryScatterToBalance
    | The threshold for the memory scatter value. When this value is reached, Hive starts balancing the load.
    | 0.5 ||
    || MinNetworkScatterToBalance
    | The threshold for the network scatter value. When this value is reached, Hive starts balancing the load.
    | 0.5 ||
    || MaxNodeUsageToKick
    | The threshold for the node resource usage. When this value is reached, Hive starts emergency balancing.
    | 0.9 ||
    || ObjectImbalanceToBalance
    | The threshold for the database object imbalance metric.
    | 0.02 ||
    |#

    {% note info %}

    These parameters use relative values, where 1.0 represents 100% and effectively disables balancing. If the total hardware resource value can exceed 100%, adjust the ratio accordingly.

    {% endnote %}


[*counter]: Counter -- a fake resource representing a count of tablets of a certain type on a node, used to ensure such tablets are distributed evenly across nodes.
