# hive_config

[Hive](../../concepts/glossary.md#hive) is a {{ ydb-short-name }} component responsible for launching [tablets](../../concepts/glossary.md#tablet). In various situations and under different load patterns, you may need to configure its behavior. Hive behavior is configured in the `hive_config` section of the {{ ydb-short-name }} [configuration](../../reference/configuration/hive_config.md). Some configuration options can also be edited through the [Hive web-viewer](../embedded-ui/hive.md#settings) interface. Settings made through the interface take precedence over those specified in the configuration. Below are all available options, with the corresponding option name in the interface if the option can be edited through the interface.

## Tablet launch options {#boot}

These options allow you to control the speed at which [tablets are launched](../../contributor/hive-booting.md) and how [nodes are selected](../../contributor/hive-booting.md#findbestnode) for them.

#|
|| Configuration parameter name | Parameter name in Hive web-viewer | Format | Description | Default value ||
|| `max_tablets_scheduled` | MaxTabletsScheduled | Integer | Maximum number of tablets simultaneously being started on a single node. | 100 ||
|| `max_boot_batch_size` | MaxBootBatchSize | Integer | Maximum number of tablets from the Hive [startup queue](../../contributor/hive-booting.md#bootqueue) processed at a time. | 1000 ||
|| `node_select_strategy` | NodeSelectStrategy | Enumeration | Node selection strategy for launching a tablet. Possible options:

- `HIVE_NODE_SELECT_STRATEGY_WEIGHTED_RANDOM` — weighted random selection based on consumption
- `HIVE_NODE_SELECT_STRATEGY_EXACT_MIN` — selection of the node with minimum consumption
- `HIVE_NODE_SELECT_STRATEGY_RANDOM_MIN_7P` — random selection among the 7% of nodes with the lowest consumption
- `HIVE_NODE_SELECT_STRATEGY_RANDOM` — random node selection.

| `HIVE_NODE_SELECT_STRATEGY_RANDOM_MIN_7P` ||
|| `boot_strategy` | — | Enumeration | Controls behavior when launching a large number of tablets. Possible options:

* `HIVE_BOOT_STRATEGY_BALANCED` — when the limit of `max_tablets_scheduled` is reached on one node, stops launching new tablets on all nodes.
* `HIVE_BOOT_STRATEGY_FAST` — when the limit of `max_tablets_scheduled` is reached on one node, continues launching tablets on other nodes.

If, when launching a large number of tablets, one node launches tablets slightly slower than others, then when using `HIVE_BOOT_STRATEGY_FAST`, fewer tablets will be launched on that node than on the others. When using `HIVE_BOOT_STRATEGY_BALANCED` in the same situation, tablets will be evenly distributed across nodes, but their launch will take longer.

| `HIVE_BOOT_STRATEGY_BALANCED` ||
|| `default_tablet_limit` | — | Nested section | Restrictions on launching tablets of different types on a single node. Specified as a list, where each element has the fields `type` and `max_count`. | Empty section ||
|| `default_tablet_preference` | —  | Nested section | Priorities for selecting data centers for launching tablets of different types. For each tablet type, you can specify multiple groups of data centers. Data centers within the same group will have the same priority, and an earlier group will have priority over subsequent ones. Example format:


```yaml
default_tablet_preference:
  - type: Coordinator
    data_centers_preference:
      - data_centers_group:
        - "dc-1"
        - "dc-2"
      - data_centers_group:
        - "dc-3"
```


| Empty section ||
|| `system_category_id` | — | Integer | When set to any non-zero value, all coordinators and mediators are launched in the same data center if possible. | 1 ||
|# {wide-content}

### Example

{% note info %}

In the subsections `default_tablet_limit` and `default_tablet_preference`, you need to specify tablet types. The exact names of tablet types are given in the [glossary](../../concepts/glossary.md#tablet-types).

{% endnote %}


```yaml
hive_config:
  max_tablets_scheduled: 10
  node_select_strategy: HIVE_NODE_SELECT_STRATEGY_RANDOM
  boot_strategy: HIVE_BOOT_STRATEGY_FAST
  default_tablet_limit:
    - type: PersQueue
      max_count: 15
    - type: DataShard
      max_count: 100
  default_tablet_preference:
    - type: Coordinator
      data_centers_preference:
        - data_centers_group:
          - "dc-1"
          - "dc-2"
        - data_centers_group:
          - "dc-3"
```


## Autobalancing options {#autobalancing}

These options control the [autobalancing](../../contributor/hive.md#autobalancing) process: in which situations it is triggered, how many tablets it moves at what intervals, how it selects nodes and tablets. Some options are available in two variants: for "emergency balancing", i.e., balancing when one or more nodes are overloaded, and for all other types of balancing.

#|
|| Configuration parameter name | Hive web-viewer parameter name | Format | Description | Default value ||
|| `min_scatter_to_balance` | MinScatterToBalance | Real number | Threshold of the [Scatter](../../contributor/hive.md#scatter) metric for CPU, Memory, Network resources. Has lower priority than the parameters below. | 0.5 ||
|| `min_cpuscatter_to_balance` | MinCPUScatterToBalance | Real number | Threshold of the Scatter metric for CPU resource. | 0.5 ||
|| `min_memory_scatter_to_balance` | MinMemoryScatterToBalance | Real number | Threshold of the Scatter metric for Memory resource. | 0.5  ||
|| `min_network_scatter_to_balance` | MinNetworkScatterToBalance | Real number | Threshold of the Scatter metric for Network resource. | 0.5 ||
|| `min_counter_scatter_to_balance` | MinCounterScatterToBalance | Real number | Threshold of the Scatter metric for the dummy resource [Counter](../../contributor/hive.md#counter). | 0.02 ||
|| `min_node_usage_to_balance` | MinNodeUsageToBalance | Real number | Resource consumption on a node below this value is considered equal to this value. Used to avoid balancing tablets between underloaded nodes. | 0.1 ||
|| `max_node_usage_to_kick` | MaxNodeUsageToKick | Real number | Threshold of resource consumption on a node to trigger emergency auto-balancing. | 0.9 ||
|| `node_usage_range_to_kick` | NodeUsageRangeToKick | Real number | Minimum difference in resource consumption level between nodes below which auto-balancing is considered impractical. | 0.2 ||
|| `resource_change_reaction_period` | ResourceChangeReactionPeriod | Integer number of seconds | Frequency of updating aggregated resource consumption statistics. | 10 ||
|| `tablet_kick_cooldown_period` | TabletKickCooldownPeriod | Integer number of seconds | Minimum time period between moves of a single tablet. | 600 ||
|| `spread_neighbours` | SpreadNeighbours | true/false | Launch tablets of the same schema object (table, topic) on different nodes if possible. | true ||
|| `node_balance_strategy` | NodeBalanceStrategy | Enumeration | Strategy for selecting the node from which tablets are moved during auto-balancing. Possible options:

- `HIVE_NODE_BALANCE_STRATEGY_WEIGHTED_RANDOM` — weighted-random selection based on consumption
- `HIVE_NODE_BALANCE_STRATEGY_HEAVIEST` — selection of the node with maximum consumption
- `HIVE_NODE_BALANCE_STRATEGY_RANDOM` — selection of a random node.

| `HIVE_NODE_BALANCE_STRATEGY_HEAVIEST` ||
|| `tablet_balance_strategy` | TabletBalanceStrategy | Enumeration | Tablet selection strategy for relocation during autobalancing. Possible options:

- `HIVE_TABLET_BALANCE_STRATEGY_WEIGHTED_RANDOM` — weighted-random selection based on consumption
- `HIVE_TABLET_BALANCE_STRATEGY_HEAVIEST` — selection of the tablet with maximum consumption
- `HIVE_TABLET_BALANCE_STRATEGY_RANDOM` — selection of a random tablet.

| `HIVE_TABLET_BALANCE_STRATEGY_WEIGHTED_RANDOM` ||
|| `min_period_between_balance` | MinPeriodBetweenBalance | Real number of seconds | Minimum time period between two auto-balancing iterations. Does not apply to emergency balancing. | 0.2 ||
|| `balancer_inflight` | BalancerInflight | Integer | Number of tablets simultaneously restarting during auto-balancing. Does not apply to emergency balancing. | 1 ||
|| `max_movements_on_auto_balancer` | MaxMovementsOnAutoBalancer | Integer | Number of tablet movements per auto-balancing iteration. Does not apply to emergency balancing. | 1 ||
|| `continue_auto_balancer` | ContinueAutoBalancer | true/false | When enabled, the next balancing iteration starts without waiting for `resource_change_reaction_period` to finish. | true ||
|| `min_period_between_emergency_balance` | MinPeriodBetweenEmergencyBalance | Real number of seconds | Similar to `min_period_between_balance`, but for emergency balancing. | 0.1 ||
|| `emergency_balancer_inflight` | EmergencyBalancerInfligh | Integer | Similar to `balancer_inflight`, but for emergency balancing. | 1 ||
|| `max_movements_on_emergency_balancer` | MaxMovementsOnEmergencyBalancer | Integer | Similar to `max_movements_on_auto_balancer`, but for emergency balancing. | 2 ||
|| `continue_emergency_balancer` | ContinueEmergencyBalancer | true/false | Similar to `continue_auto_balancer`, but for emergency balancing. | true ||
|| `check_move_expediency` | CheckMoveExpediency | true/false | Check the expediency of tablet movements. If auto-balancing leads to increased CPU consumption by Hive, you can disable this option. | true ||
|| `object_imbalance_to_balance` | ObjectImbalanceToBalance | Real number | Threshold metric for [imbalance of tablets of one object](../../contributor/hive.md#imbalance). | 0.02 ||
|| `less_system_tablets_moves` | LessSystemTabletMoves | true/false | Minimize movement of system tablets during auto-balancing. | true ||
|| `balancer_ignore_tablet_types` | BalancerIgnoreTabletTypes | List of tablet types. When set via Hive UI — semicolon-separated. | Tablet types not subject to auto-balancing. | Empty list ||
|# {wide-content}

### Examples

Using such a configuration file, you can completely disable all types of tablet autobalancing between nodes.


```yaml
hive_config:
  min_cpuscatter_to_balance: 1.0
  min_memory_scatter_to_balance: 1.0
  min_network_scatter_to_balance: 1.0
  min_counter_scatter_to_balance: 1.0
  max_node_usage_to_kick: 3.0
  object_imbalance_to_balance: 1.0
```


With such a configuration file, you can disable all types of autobalancing between nodes for tablets involved in transaction distribution, i.e., [coordinators](../../concepts/glossary.md#coordinator) and [mediators](../../concepts/glossary.md#mediator). The exact names of tablet types are given in the [glossary](../../concepts/glossary.md#tablet-types).


```yaml
hive_config:
  balancer_ignore_tablet_types:
    - Coordinator
    - Mediator
```


When using Hive UI, to achieve the same effect, you need to enter `Coordinator;Mediator` in the input field for the BalancerIgnoreTabletTypes setting.

## Compute resource consumption metric collection options {metrics}

Hive collects [compute resource consumption metrics](../../contributor/hive.md#resources) from each node — CPU time, RAM, network — both overall for the node and broken down by tablet. These settings allow you to regulate the collection of these metrics, their normalization, and aggregation.

#|
|| Configuration parameter name | Hive web-viewer parameter name | Format | Description | Default value ||
|| `max_resource_cpu` | MaxResourceCPU | Integer, microseconds | Maximum CPU consumption per node per second. Default value, used only if the node does not provide a value when registering with Hive. | 10000000 ||
|| `max_resource_memory` | MaxResourceMemory | Integer, bytes | Maximum memory consumption per node. Default value, used only if the node does not provide a value when registering with Hive. | 512000000000 ||
|| `max_resource_network` | MaxResourceNetwork | Integer, bytes/second | Maximum bandwidth consumption per node. Default value, used only if the node does not provide a value when registering with Hive. | 1000000000 ||
|| `max_resource_counter` | MaxResourceCounter | Integer | Maximum consumption of the virtual Counter resource per node. | 100000000 ||
|| `metrics_window_size` | MetricsWindowSize | Integer, milliseconds | Window size over which tablet resource consumption metrics are aggregated. | 60000 ||
|| `resource_overcommitment` | ResourceOvercommitment | Real number | Node resource overcommitment coefficient. | 3.0 ||
|| `pools_to_monitor_for_usage` | — | Pool names, comma-separated | Actor system pools whose consumption is taken into account when calculating node resource consumption. | System,User,IC ||
|# {wide-content}

## Channel distribution options among storage groups {#storage}

This section lists options related to the distribution of tablet [channels](../../concepts/glossary.md#channel) across [storage groups](../../concepts/glossary.md#storage-group): taking into account various metrics, group selection, and the process of autobalancing channels across groups.

{% note info %}

This table contains advanced settings that do not require modification in most cases.

{% endnote %}

#|
|| Configuration parameter name | Hive web-viewer parameter name | Format | Description | Default value ||
|| `default_unit_iops` | DefaultUnitIOPS | Integer | Default IOPS value for one channel. | 1 ||
|| `default_unit_throughput` | DefaultUnitThroughput | Integer, bytes/second | Default bandwidth consumption value for one channel. | 1000 ||
|| `default_unit_size` | DefaultUnitSize | Integer, bytes | Default disk space consumption value for one channel. | 100000000 ||
|| `storage_overcommit` | StorageOvercommit | Real number | Storage group resource overcommitment coefficient. | 1.0 ||
|| `storage_balance_strategy` | StorageBalanceStrategy | Enumeration | Selection of the parameter used for distributing tablet channels across storage groups. Possible options:

- `HIVE_STORAGE_BALANCE_STRATEGY_IOPS` — only IOPS is considered.
- `HIVE_STORAGE_BALANCE_STRATEGY_THROUGHPUT` — only bandwidth consumption is considered.
- `HIVE_STORAGE_BALANCE_STRATEGY_SIZE` — only occupied disk space is considered.
- `HIVE_STORAGE_BALANCE_STRATEGY_AUTO` — the one of the above parameters whose consumption is maximum is considered.

| `HIVE_STORAGE_BALANCE_STRATEGY_SIZE` ||
|| `storage_safe_mode` | StorageSafeMode | true/false | Check for exceeding the maximum resource consumption of storage groups. | true ||
|| `storage_select_strategy` | StorageSelectStrategy | Enumeration | Strategy for selecting a storage group for a tablet channel. Possible options:

- `HIVE_STORAGE_SELECT_STRATEGY_WEIGHTED_RANDOM` — weighted random selection based on consumption.
- `HIVE_STORAGE_SELECT_STRATEGY_EXACT_MIN` — selection of the group with minimum consumption.
- `HIVE_STORAGE_SELECT_STRATEGY_RANDOM_MIN_7P` — random selection among the 7% of groups with the lowest consumption.
- `HIVE_STORAGE_SELECT_STRATEGY_RANDOM` — random group selection.
- `HIVE_STORAGE_SELECT_STRATEGY_ROUND_ROBIN` — selection of a group within a storage pool using the [Round-robin](https://en.wikipedia.org/wiki/Round-robin_scheduling) principle.
  | `HIVE_STORAGE_SELECT_STRATEGY_WEIGHTED_RANDOM` ||
  || `min_period_between_reassign` | MinPeriodBetweenReassign | Integer number of seconds | Minimum time period between reassignments of storage groups for channels of a single tablet. | 300 ||
  || `storage_pool_fresh_period` | StoragePoolFreshPeriod | Integer number of milliseconds | Frequency of updating information about storage pools. | 60000 ||
  || `space_usage_penalty_threshold` | SpaceUsagePenaltyThreshold | Floating-point number | Minimum ratio of free space in the target group to free space in the source group, at which the target group will be penalized by applying a multiplicative penalty to the weight during channel transfer. | 1.1 ||
  || `space_usage_penalty` | SpaceUsagePenalty | Floating-point number | Penalty coefficient for the pessimization described above. | 0.2 ||
  || `channel_balance_strategy` | ChannelBalanceStrategy | Enumeration | Strategy for selecting a channel for reassignment during channel balancing. Possible options:
- `HIVE_CHANNEL_BALANCE_STRATEGY_WEIGHTED_RANDOM` — Weighted random selection based on consumption.
- `HIVE_CHANNEL_BALANCE_STRATEGY_HEAVIEST` — Selection of the channel with maximum consumption.
- `HIVE_CHANNEL_BALANCE_STRATEGY_RANDOM` — Selection of a random channel.

| `HIVE_CHANNEL_BALANCE_STRATEGY_WEIGHTED_RANDOM` ||
|| `max_channel_history_size` |  MaxChannelHistorySize | Integer | Maximum channel history size. | 200 ||
|| `storage_info_refresh_frequency` | StorageInfoRefreshFrequency | Integer (milliseconds) | Frequency of updating storage pool information. | 600000 ||
|| `min_storage_scatter_to_balance` | MinStorageScatterToBalance | Real number | Scatter metric threshold for storage groups. | 999 ||
|| `min_group_usage_to_balance` | MinGroupUsageToBalance | Real number | Resource consumption threshold for a storage group below which balancing is not started. | 0.1 ||
|| `storage_balancer_inflight` | StorageBalancerInflight | Integer | Number of tablets simultaneously restarted during channel balancing. | 1 ||
|# {wide-content}

## Restart tracking options {#restarts}

Hive tracks how often various nodes and tablets restart to identify problematic ones. Using these options, you can configure which tablets or nodes are considered problematic and how this affects them. Based on this statistics, nodes and tablets are included in the [HealthCheck API](../ydb-sdk/health-check-api.md) report.

### Tablet restart tracking options

#|
|| Parameter name in configuration | Parameter name in Hive web-viewer | Format | Description | Default value ||
|| `tablet_restart_watch_period` | — | Integer (seconds) | Window size for collecting statistics on the number of tablet restarts. **This period is used only for statistics passed to HealthCheck.** | 3600 ||
|| `tablet_restarts_period` | — | Integer (milliseconds) | Window size for counting the number of tablet restarts to pessimize the launch of problematic tablets. | 1000 ||
|| `tablet_restarts_max_count` | — | Integer | Number of restarts in the `tablet_restarts_period` window, above which pessimization is applied. | 2 ||
|| `postopone_start_period` | — | Integer (milliseconds) | Frequency of attempts to start problematic tablets. | 1000 ||
|# {wide-content}

### Node restart tracking options

#|
|| Parameter name in configuration | Parameter name in Hive web-viewer | Format | Description | Default value ||
|| `node_restart_watch_period` | — | Integer (seconds) | Window size for collecting statistics on the number of node restarts. | 3600 ||
|| `node_restarts_for_penalty` | NodeRestartsForPenalty | Integer | Number of restarts in the `node_restart_watch_period` window after which nodes receive a priority decrease. | 3 ||
|# {wide-content}

## Other {#misc}

Additional Hive settings are listed here.

{% note info %}

This table contains advanced settings that typically do not require modification.

{% endnote %}

#|
|| Parameter name in configuration | Parameter name in Hive web-viewer | Format | Description | Default value ||
|| `drain_inflight` | DrainInflight | Integer | Number of tablets simultaneously restarted during the smooth drain of all tablets from a node. | 10 ||
|| `request_sequence_size` | — | Integer | Number of tablet IDs that a database Hive requests from the root Hive at a time. | 1000 ||
|| `min_request_sequence_size` | — | Integer | Minimum number of tablet IDs that the root Hive allocates to a database Hive at a time. | 1000 ||
|| `max_request_sequence_size` | — | Integer | Maximum number of tablet IDs allocated to a database Hive at a time. | 1000000 ||
|| `node_delete_period` | — | Integer (seconds) | Inactivity period after which a node is removed from the Hive database. | 3600 ||
|| `warm_up_enabled` | WarmUpEnabled | true/false | When enabled, the Hive database waits for all nodes to connect before starting tablets. When disabled, all tablets may be started on the first node that connects. | true ||
|| `warm_up_boot_waiting_period` | MaxWarmUpBootWaitingPeriod | Integer (milliseconds) | Time to wait for all known nodes to start when the database starts. | 30000 ||
|| `max_warm_up_period` | MaxWarmUpPeriod | Integer (seconds) | Maximum time to wait for nodes to start when the database starts. | 600 ||
|| `enable_destroy_operations` | — | true/false | Whether destructive manual operations are allowed. | false ||
|| `max_pings_in_flight` | — | Integer | Maximum number of concurrently established connections to nodes. | 1000 ||
|| `cut_history_deny_list` | — | Comma-separated list of tablet types | List of tablet types for which the history cleanup operation is ignored. | ColumnShard,KeyValue,PersQueue,BlobDepot ||
|| `cut_history_allow_list` | — | Comma-separated list of tablet types | List of tablet types for which the history cleanup operation is allowed. | DataShard ||
|| `scale_recommendation_refresh_frequency` | ScaleRecommendationRefreshFrequency | Integer (milliseconds) | How often the recommendation for the number of compute nodes is updated. | 60000 ||
|| `scale_out_window_size` | ScaleOutWindowSize | Integer | Number of buckets based on which the decision to recommend increasing the number of compute nodes is made. | 15 ||
|| `scale_in_window_size` | ScaleInWindowSize | Integer | Number of buckets based on which the decision to recommend decreasing the number of compute nodes is made. |5 ||
|| `target_tracking_cpumargin` | TargetTrackingCPUMargin | Floating-point number | Allowed deviation from the target CPU utilization value during autoscaling. | 0.1 ||
|| `dry_run_target_tracking_cpu` | DryRunTargetTrackingCPU | Floating-point number | Target CPU utilization value to test how autoscaling would work. | 0 ||
|# {wide-content}
