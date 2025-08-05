# hive_config

[Hive](../../concepts/glossary.md#hive) is a YDB component responsible for launching [tablets](../../concepts/glossary.md#tablet). In various situations and under different load patterns, you might need to configure its behavior. Hive behavior is configured in the `hive_config` section of the {{ ydb-short-name }} [configuration](../../reference/configuration/hive.md). Some configuration options are also available for editing through the [Hive web-viewer](../embedded-ui/hive.md#settings) interface. Settings configured through the interface take priority over those specified in the configuration. Below are all available options, with the corresponding option name in the interface indicated if the option can be edited through the interface.

## Tablet Boot Options {#boot}

These options allow you to control the speed at which [tablets are booted](../../contributor/hive-booting.md) and how [nodes are selected](../../contributor/hive-booting.md#findbestnode) for them.

#|
|| Configuration Parameter Name | Hive Web-viewer Parameter Name | Format | Description | Default Value ||
|| `max_tablets_scheduled` | MaxTabletsScheduled | Integer | Maximum number of tablets simultaneously in the startup process on a single node. | 100 ||
|| `max_boot_batch_size` | MaxBootBatchSize | Integer | Maximum number of tablets from the Hive [boot queue](../../contributor/hive-booting.md#bootqueue) processed at once. | 1000 ||
|| `node_select_strategy` | NodeSelectStrategy | Enumeration | Node selection strategy for tablet startup. Possible values:

- `HIVE_NODE_SELECT_STRATEGY_WEIGHTED_RANDOM` — weighted random selection based on consumption;
- `HIVE_NODE_SELECT_STRATEGY_EXACT_MIN` — select node with minimum consumption;
- `HIVE_NODE_SELECT_STRATEGY_RANDOM_MIN_7P` — select random node among 7% of nodes with lowest consumption;
- `HIVE_NODE_SELECT_STRATEGY_RANDOM` — select random node.

| `HIVE_NODE_SELECT_STRATEGY_RANDOM_MIN_7P` ||
|| `boot_strategy` | — | Enumeration | Controls behavior when starting large numbers of tablets. Possible values:

* `HIVE_BOOT_STRATEGY_BALANCED` — when the `max_tablets_scheduled` limit is reached on one node, stops starting new tablets on all nodes.
* `HIVE_BOOT_STRATEGY_FAST` — when the `max_tablets_scheduled` limit is reached on one node, continues starting tablets on other nodes.

If when starting a large number of tablets one node starts tablets slightly slower than others, then using `HIVE_BOOT_STRATEGY_FAST` will result in fewer tablets being started on that node than on the others. Using `HIVE_BOOT_STRATEGY_BALANCED` in the same situation will distribute tablets evenly across nodes, but their startup will take longer.

| `HIVE_BOOT_STRATEGY_BALANCED` ||
|| `default_tablet_limit` | — | Nested section | Limits on starting tablets of various types on a single node. Specified as a list format where each element has `type` and `max_count` fields. | Empty section ||
|| `default_tablet_preference` | — | Nested section | Priorities for selecting data centers for starting tablets of various types. For each tablet type, you can specify multiple data center groups. Data centers within the same group will have equal priority, with earlier groups having priority over subsequent ones. Example format:

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
|| `system_category_id` | — | Integer | When specifying any number other than 0, all coordinators and mediators are launched in the same data center whenever possible. | 1 ||
|# {wide-content}

### Example

{% note info %}

In the `default_tablet_limit` and `default_tablet_preference` subsections, you need to specify tablet types. Exact tablet type names are specified in the [glossary](../../concepts/glossary.md#tablet-types).

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

## Auto-balancing Options {#autobalancing}

These options control the [auto-balancing](../../contributor/hive.md#autobalancing) process: in which situations it starts, how many tablets it moves at what intervals, how it selects nodes and tablets. Some options are presented in two variations: for "emergency balancing," i.e., balancing when one or more nodes are overloaded, and for all other types of balancing.

#|
|| Configuration Parameter Name | Hive Web-viewer Parameter Name | Format | Description | Default Value ||
|| `min_scatter_to_balance` | MinScatterToBalance | Real number | Threshold for [Scatter](../../contributor/hive.md#scatter) metric for CPU, Memory, Network resources. Has lower priority than the parameters below. | 0.5 ||
|| `min_cpuscatter_to_balance` | MinCPUScatterToBalance | Real number | Threshold for Scatter metric for CPU resource. | 0.5 ||
|| `min_memory_scatter_to_balance` | MinMemoryScatterToBalance | Real number | Threshold for Scatter metric for Memory resource. | 0.5 ||
|| `min_network_scatter_to_balance` | MinNetworkScatterToBalance | Real number | Threshold for Scatter metric for Network resource. | 0.5 ||
|| `min_counter_scatter_to_balance` | MinCounterScatterToBalance | Real number | Threshold for Scatter metric for virtual [Counter](../../contributor/hive.md#counter) resource. | 0.02 ||
|| `min_node_usage_to_balance` | MinNodeUsageToBalance | Real number | Resource consumption on a node below this value is equated to this value. Used to avoid balancing tablets between lightly loaded nodes. | 0.1 ||
|| `max_node_usage_to_kick` | MaxNodeUsageToKick | Real number | Resource consumption threshold on a node for triggering emergency auto-balancing. | 0.9 ||
|| `node_usage_range_to_kick` | NodeUsageRangeToKick | Real number | Minimum difference in resource consumption level between nodes, below which auto-balancing is considered inappropriate. | 0.2 ||
|| `resource_change_reaction_period` | ResourceChangeReactionPeriod | Integer seconds | Frequency of updating aggregated resource consumption statistics. | 10 ||
|| `tablet_kick_cooldown_period` | TabletKickCooldownPeriod | Integer seconds | Minimum time period between movements of a single tablet. | 600 ||
|| `spread_neighbours` | SpreadNeighbours | true/false | Start tablets of the same schema object (table, topic) on different nodes when possible. | true ||
|| `node_balance_strategy` | NodeBalanceStrategy | Enumeration | Strategy for selecting the node from which tablets are moved during auto-balancing. Possible values:

- `HIVE_NODE_BALANCE_STRATEGY_WEIGHTED_RANDOM` — weighted random selection based on consumption;
- `HIVE_NODE_BALANCE_STRATEGY_HEAVIEST` — select node with maximum consumption;
- `HIVE_NODE_BALANCE_STRATEGY_RANDOM` — select random node.

| `HIVE_NODE_BALANCE_STRATEGY_HEAVIEST` ||
|| `tablet_balance_strategy` | TabletBalanceStrategy | Enumeration | Strategy for selecting tablet to move during auto-balancing. Possible values:

- `HIVE_TABLET_BALANCE_STRATEGY_WEIGHTED_RANDOM` — weighted random selection based on consumption;
- `HIVE_TABLET_BALANCE_STRATEGY_HEAVIEST` — select tablet with maximum consumption;
- `HIVE_TABLET_BALANCE_STRATEGY_RANDOM` — select random tablet.

| `HIVE_TABLET_BALANCE_STRATEGY_WEIGHTED_RANDOM` ||
|| `min_period_between_balance` | MinPeriodBetweenBalance | Real number seconds | Minimum time period between two auto-balancing iterations. Does not apply to emergency balancing. | 0.2 ||
|| `balancer_inflight` | BalancerInflight | Integer | Number of tablets simultaneously restarting during auto-balancing process. Does not apply to emergency balancing. | 1 ||
|| `max_movements_on_auto_balancer` | MaxMovementsOnAutoBalancer | Integer | Number of tablet movements per auto-balancing iteration. Does not apply to emergency balancing. | 1 ||
|| `continue_auto_balancer` | ContinueAutoBalancer | true/false | When enabled, the next balancing iteration starts without waiting for the end of `resource_change_reaction_period`. | true ||
|| `min_period_between_emergency_balance` | MinPeriodBetweenEmergencyBalance | Real number seconds | Similar to `min_period_between_balance`, but for emergency balancing. | 0.1 ||
|| `emergency_balancer_inflight` | EmergencyBalancerInfligh | Integer | Similar to `balancer_inflight`, but for emergency balancing. | 1 ||
|| `max_movements_on_emergency_balancer` | MaxMovementsOnEmergencyBalancer | Integer | Similar to `max_movements_on_auto_balancer`, but for emergency balancing. | 2 ||
|| `continue_emergency_balancer` | ContinueEmergencyBalancer | true/false | Similar to `continue_auto_balancer`, but for emergency balancing. | true ||
|| `check_move_expediency` | CheckMoveExpediency | true/false | Check the expediency of tablet movements. If auto-balancing leads to increased Hive CPU resource consumption, you can disable this option. | true ||
|| `object_imbalance_to_balance` | ObjectImbalanceToBalance | Real number | Threshold for [single object tablet imbalance](../../contributor/hive.md#imbalance) metric. | 0.02 ||
|| `less_system_tablets_moves` | LessSystemTabletMoves | true/false | Minimize movement of system tablets during auto-balancing. | true ||
|| `balancer_ignore_tablet_types` | BalancerIgnoreTabletTypes | List of tablet types. When set through Hive UI — separated by semicolon. | Tablet types that are not subject to auto-balancing. | Empty list ||
|# {wide-content}

### Examples

With this configuration file, you can completely disable all types of tablet auto-balancing between nodes.

```yaml
hive_config:
  min_cpuscatter_to_balance: 1.0
  min_memory_scatter_to_balance: 1.0
  min_network_scatter_to_balance: 1.0
  min_counter_scatter_to_balance: 1.0
  max_node_usage_to_kick: 3.0
  object_imbalance_to_balance: 1.0
```

With this configuration file, you can disable all types of auto-balancing between nodes for tablets participating in transaction distribution, i.e., [coordinators](../../concepts/glossary.md#coordinator) and [mediators](../../concepts/glossary.md#mediator). Exact tablet type names are specified in the [glossary](../../concepts/glossary.md#tablet-types).

```yaml
hive_config:
  balancer_ignore_tablet_types:
    - Coordinator
    - Mediator
```

When using Hive UI for the same effect, you need to enter `Coordinator;Mediator` in the input field for the BalancerIgnoreTabletTypes setting.

## Computational Resource Consumption Metrics Collection Options {#metrics}

Hive collects [computational resource consumption metrics](../../contributor/hive.md#resources) from each node — CPU time, memory, network — both overall per node and broken down by tablets. These settings allow you to control the collection of these metrics, their normalization and aggregation.

#|
|| Configuration Parameter Name | Hive Web-viewer Parameter Name | Format | Description | Default Value ||
|| `max_resource_cpu` | MaxResourceCPU | Integer microseconds | Maximum CPU consumption per node per second. Default value, used only if the node does not provide a value when registering with Hive. | 10000000 ||
|| `max_resource_memory` | MaxResourceMemory | Integer bytes | Maximum memory consumption per node. Default value, used only if the node does not provide a value when registering with Hive. | 512000000000 ||
|| `max_resource_network` | MaxResourceNetwork | Integer bytes/second | Maximum bandwidth consumption per node. Default value, used only if the node does not provide a value when registering with Hive. | 1000000000 ||
|| `max_resource_counter` | MaxResourceCounter | Integer | Maximum consumption of virtual Counter resource per node. | 100000000 ||
|| `metrics_window_size` | MetricsWindowSize | Integer milliseconds | Size of the window over which tablet resource consumption metrics are aggregated. | 60000 ||
|| `resource_overcommitment` | ResourceOvercommitment | Real number | Overcommitment factor for node resources. | 3.0 ||
|| `pools_to_monitor_for_usage` | — | Pool names separated by comma | Actor system pools whose consumption is taken into account when calculating node resource consumption. | System,User,IC ||
|# {wide-content}

## Storage Channel Distribution Options {#storage}

Listed here are options related to distributing tablet [channels](../../concepts/glossary.md#channel) across [storage groups](../../concepts/glossary.md#storage-group): taking into account various metrics, selecting groups, and the channel auto-balancing process across groups.

{% note info %}

This table contains advanced settings that in most cases do not require modification.

{% endnote %}

#|
|| Configuration Parameter Name | Hive Web-viewer Parameter Name | Format | Description | Default Value ||
|| `default_unit_iops` | DefaultUnitIOPS | Integer | Default value for IOPS of one channel. | 1 ||
|| `default_unit_throughput` | DefaultUnitThroughput | Integer bytes/second | Default value for throughput consumption by one channel. | 1000 ||
|| `default_unit_size` | DefaultUnitSize | Integer bytes | Default value for disk space consumption by one channel. | 100000000 ||
|| `storage_overcommit` | StorageOvercommit | Real number | Overcommitment factor for storage group resources. | 1.0 ||
|| `storage_balance_strategy` | StorageBalanceStrategy | Enumeration | Selection of parameter used for distributing tablet channels across storage groups. Possible values:

- `HIVE_STORAGE_BALANCE_STRATEGY_IOPS` — only IOPS is considered;
- `HIVE_STORAGE_BALANCE_STRATEGY_THROUGHPUT` — only throughput consumption is considered;
- `HIVE_STORAGE_BALANCE_STRATEGY_SIZE` — only occupied space volume is considered;
- `HIVE_STORAGE_BALANCE_STRATEGY_AUTO` — the parameter with maximum consumption among the above is considered;

| `HIVE_STORAGE_BALANCE_STRATEGY_SIZE` ||
|| `storage_safe_mode` | StorageSafeMode | true/false | Check for exceeding maximum resource consumption of storage groups. | true ||
|| `storage_select_strategy` | StorageSelectStrategy | Enumeration | Strategy for selecting storage group for tablet channel. Possible values:

- `HIVE_STORAGE_SELECT_STRATEGY_WEIGHTED_RANDOM` — weighted random selection based on consumption;
- `HIVE_STORAGE_SELECT_STRATEGY_EXACT_MIN` — select group with minimum consumption;
- `HIVE_STORAGE_SELECT_STRATEGY_RANDOM_MIN_7P` — select random group among 7% of groups with lowest consumption;
- `HIVE_STORAGE_SELECT_STRATEGY_RANDOM` — select random group;
- `HIVE_STORAGE_SELECT_STRATEGY_ROUND_ROBIN` — select group within storage pool using [Round-robin](https://en.wikipedia.org/wiki/Round-robin_scheduling) principle.
| `HIVE_STORAGE_SELECT_STRATEGY_WEIGHTED_RANDOM` ||
|| `min_period_between_reassign` | MinPeriodBetweenReassign | Integer seconds | Minimum time period between storage group reassignments for channels of one tablet. | 300 ||
|| `storage_pool_fresh_period` | StoragePoolFreshPeriod | Integer milliseconds | Frequency of updating storage pool information. | 60000 ||
|| `space_usage_penalty_threshold` | SpaceUsagePenaltyThreshold | Real number | Minimum ratio of free space in target group to free space in source group, at which the target group will be penalized by applying a multiplicative penalty to the weight when moving a channel. | 1.1 ||
|| `space_usage_penalty` | SpaceUsagePenalty | Real number | Penalty factor for the penalization described above. | 0.2 ||
|| `channel_balance_strategy` | ChannelBalanceStrategy | Enumeration | Strategy for selecting channel for reassignment during channel balancing. Possible values:

- `HIVE_CHANNEL_BALANCE_STRATEGY_WEIGHTED_RANDOM` — weighted random selection based on consumption;
- `HIVE_CHANNEL_BALANCE_STRATEGY_HEAVIEST` — select channel with maximum consumption;
- `HIVE_CHANNEL_BALANCE_STRATEGY_RANDOM` — select random channel.

| `HIVE_CHANNEL_BALANCE_STRATEGY_WEIGHTED_RANDOM` ||
|| `max_channel_history_size` | MaxChannelHistorySize | Integer | Maximum size of channel history. | 200 ||
|| `storage_info_refresh_frequency` | StorageInfoRefreshFrequency | Integer milliseconds | Frequency of updating storage pool information. | 600000 ||
|| `min_storage_scatter_to_balance` | MinStorageScatterToBalance | Real number | Threshold for Scatter metric for storage groups. | 999 ||
|| `min_group_usage_to_balance` | MinGroupUsageToBalance | Real number | Storage group resource consumption threshold below which balancing is not started. | 0.1 ||
|| `storage_balancer_inflight` | StorageBalancerInflight | Integer | Number of tablets simultaneously restarting during channel balancing. | 1 ||
|# {wide-content}

## Restart Tracking Options {#restarts}

Hive tracks how often various nodes and tablets restart to identify problematic ones. Using these options, you can configure which tablets or nodes will be considered problematic and how this will affect them. Based on this statistics, nodes and tablets are included in the [HealthCheck API](../ydb-sdk/health-check-api.md) report.

### Tablet Restart Tracking Options

#|
|| Configuration Parameter Name | Hive Web-viewer Parameter Name | Format | Description | Default Value ||
|| `tablet_restart_watch_period` | — | Integer seconds | Size of window over which statistics on tablet restart count are collected. **This period is used only for statistics passed to HealthCheck.** | 3600 ||
|| `tablet_restarts_period` | — | Integer milliseconds | Size of window over which tablet restart count is calculated for penalizing problematic tablet startup. | 1000 ||
|| `tablet_restarts_max_count` | — | Integer | Number of restarts in the `tablet_restarts_period` window, exceeding which triggers penalization. | 2 ||
|| `postopone_start_period` | — | Integer milliseconds | Frequency of startup attempts for problematic tablets. | 1000 ||
|# {wide-content}

### Node Restart Tracking Options

#|
|| Configuration Parameter Name | Hive Web-viewer Parameter Name | Format | Description | Default Value ||
|| `node_restart_watch_period` | — | Integer seconds | Size of window over which statistics on node restart count are collected. | 3600 ||
|| `node_restarts_for_penalty` | NodeRestartsForPenalty | Integer | Number of restarts in the `node_restart_watch_period` window after which nodes receive priority reduction. | 3 ||
|# {wide-content}

## Miscellaneous {#misc}

Listed here are additional Hive settings.

{% note info %}

This table contains advanced settings that in most cases do not require modification.

{% endnote %}

#|
|| Configuration Parameter Name | Hive Web-viewer Parameter Name | Format | Description | Default Value ||
|| `drain_inflight` | DrainInflight | Integer | Number of tablets simultaneously restarting during graceful movement of all tablets from one node (drain). | 10 ||
|| `request_sequence_size` | — | Integer | Number of tablet identifiers that database Hive requests from root Hive at once. | 1000 ||
|| `min_request_sequence_size` | — | Integer | Minimum number of tablet identifiers that root Hive allocates for database Hive at once. | 1000 ||
|| `max_request_sequence_size` | — | Integer | Maximum number of tablet identifiers allocated for database Hive at once. | 1000000 ||
|| `node_delete_period` | — | Integer seconds | Inactivity period after which a node is deleted from the Hive database. | 3600 ||
|| `warm_up_enabled` | WarmUpEnabled | true/false | When this option is enabled, database Hive waits for all nodes to connect before starting tablets during startup. When disabled, all tablets can be started on the first connected node. | true ||
|| `warm_up_boot_waiting_period` | MaxWarmUpBootWaitingPeriod | Integer milliseconds | Waiting time for all known nodes to start during database startup. | 30000 ||
|| `max_warm_up_period` | MaxWarmUpPeriod | Integer seconds | Maximum waiting time for node startup during database startup. | 600 ||
|| `enable_destroy_operations` | — | true/false | Whether destructive manual operations are allowed. | false ||
|| `max_pings_in_flight` | — | Integer | Maximum number of connections being established with nodes in parallel. | 1000 ||
|| `cut_history_deny_list` | — | List of tablet types separated by comma | List of tablet types for which history cleanup operation is ignored. | ColumnShard,KeyValue,PersQueue,BlobDepot ||
|| `cut_history_allow_list` | — | List of tablet types separated by comma | List of tablet types for which history cleanup operation is allowed. | DataShard ||
|| `scale_recommendation_refresh_frequency` | ScaleRecommendationRefreshFrequency | Integer milliseconds | How often the recommendation for the number of compute nodes is updated. | 60000 ||
|| `scale_out_window_size` | ScaleOutWindowSize | Integer | Number of buckets based on which the decision to recommend increasing the number of compute nodes is made. | 15 ||
|| `scale_in_window_size` | ScaleInWindowSize | Integer | Number of buckets based on which the decision to recommend decreasing the number of compute nodes is made. | 5 ||
|| `target_tracking_cpumargin` | TargetTrackingCPUMargin | Real number | Allowable deviation from target CPU utilization value during autoscaling. | 0.1 ||
|| `dry_run_target_tracking_cpu` | DryRunTargetTrackingCPU | Real number | Target CPU utilization value for testing how autoscaling would work. | 0 ||
|# {wide-content}