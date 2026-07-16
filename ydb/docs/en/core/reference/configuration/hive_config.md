# hive_config

[Hive](../../concepts/glossary.md#hive) is the {{ ydb-short-name }} component responsible for starting [tablets](../../concepts/glossary.md#tablet). You may need to tune its behavior for specific situations and different load patterns. Hive behavior is configured in the `hive_config` section of the {{ ydb-short-name }} [configuration](../../reference/configuration/hive_config.md).

Some configuration options can also be edited through the [Hive web-viewer](../embedded-ui/hive.md#settings). Settings specified through the interface take precedence over the configuration file. All available options are listed below, together with the corresponding interface option name when the option can be edited through the interface.

## Tablet Startup Options {#boot}

These options control how fast [tablets are started](../../contributor/hive-booting.md) and how [nodes are selected](../../contributor/hive-booting.md#findbestnode) for them.

#|
|| Configuration parameter name | Hive web-viewer parameter name | Format | Description | Default value ||
|| `max_tablets_scheduled` | MaxTabletsScheduled | Integer | Maximum number of tablets that can be in startup on a single node at the same time. | 100 ||
|| `max_boot_batch_size` | MaxBootBatchSize | Integer | Maximum number of tablets from the Hive [startup queue](../../contributor/hive-booting.md#bootqueue) processed at a time. | 1000 ||
|| `node_select_strategy` | NodeSelectStrategy | Enumeration | Strategy for selecting a node to start a tablet. Valid values:

- `HIVE_NODE_SELECT_STRATEGY_WEIGHTED_RANDOM`: Weighted random selection based on resource usage.
- `HIVE_NODE_SELECT_STRATEGY_EXACT_MIN`: Selects the node with the lowest resource usage.
- `HIVE_NODE_SELECT_STRATEGY_RANDOM_MIN_7P`: Selects a random node from the 7% of nodes with the lowest resource usage.
- `HIVE_NODE_SELECT_STRATEGY_RANDOM`: Selects a random node.

| `HIVE_NODE_SELECT_STRATEGY_RANDOM_MIN_7P` ||
|| `boot_strategy` | — | Enumeration | Controls behavior when starting a large number of tablets. Valid values:

* `HIVE_BOOT_STRATEGY_BALANCED`: When the `max_tablets_scheduled` limit is reached on one node, stops starting new tablets on all nodes.
* `HIVE_BOOT_STRATEGY_FAST`: When the `max_tablets_scheduled` limit is reached on one node, continues starting tablets on other nodes.

If one node starts tablets slightly slower than others while a large number of tablets are being started, then with `HIVE_BOOT_STRATEGY_FAST`, fewer tablets will be started on that node than on the others. With `HIVE_BOOT_STRATEGY_BALANCED` in the same situation, tablets will be distributed evenly across nodes, but startup will take longer.

| `HIVE_BOOT_STRATEGY_BALANCED` ||
|| `default_tablet_limit` | — | Nested section | Limits for starting tablets of different types on a single node. Specify this setting as a list, where each item has the `type` and `max_count` fields. | Empty section ||
|| `default_tablet_preference` | —  | Nested section | Data center selection priorities for starting tablets of different types. You can specify several data center groups for each tablet type. Data centers within one group have equal priority, and an earlier group has priority over later groups. Format example:

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
|| `system_category_id` | — | Integer | If set to any non-zero value, all coordinators and mediators are started in the same data center whenever possible. | 1 ||
|# {wide-content}

### Example

{% note info %}

In the `default_tablet_limit` and `default_tablet_preference` subsections, specify tablet types. Exact tablet type names are listed in the [glossary](../../concepts/glossary.md#tablet-types).

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

## Autobalancing Options {#autobalancing}

These options control the [autobalancing](../../contributor/hive.md#autobalancing) process: when it starts, how many tablets it moves and at what intervals, and how it selects nodes and tablets. Some options have two variants: one for emergency balancing, which is balancing performed when one or more nodes are overloaded, and one for all other balancing types.

#|
|| Configuration parameter name | Hive web-viewer parameter name | Format | Description | Default value ||
|| `min_scatter_to_balance` | MinScatterToBalance | Floating-point number | [Scatter](../../contributor/hive.md#scatter) metric threshold for CPU, Memory, and Network resources. Has lower priority than the parameters below. | 0.5 ||
|| `min_cpuscatter_to_balance` | MinCPUScatterToBalance | Floating-point number | Scatter metric threshold for the CPU resource. | 0.5 ||
|| `min_memory_scatter_to_balance` | MinMemoryScatterToBalance | Floating-point number | Scatter metric threshold for the Memory resource. | 0.5  ||
|| `min_network_scatter_to_balance` | MinNetworkScatterToBalance | Floating-point number | Scatter metric threshold for the Network resource. | 0.5 ||
|| `min_counter_scatter_to_balance` | MinCounterScatterToBalance | Floating-point number | Scatter metric threshold for the virtual [Counter](../../contributor/hive.md#counter) resource. | 0.02 ||
|| `min_node_usage_to_balance` | MinNodeUsageToBalance | Floating-point number | Resource usage on a node below this value is treated as equal to this value. This prevents tablets from being balanced between lightly loaded nodes. | 0.1 ||
|| `max_node_usage_to_kick` | MaxNodeUsageToKick | Floating-point number | Node resource usage threshold for starting emergency autobalancing. | 0.9 ||
|| `node_usage_range_to_kick` | NodeUsageRangeToKick | Floating-point number | Minimum difference in resource usage levels between nodes. Below this difference, autobalancing is considered impractical. | 0.2 ||
|| `resource_change_reaction_period` | ResourceChangeReactionPeriod | Integer, seconds | Frequency for updating aggregated resource usage statistics. | 10 ||
|| `tablet_kick_cooldown_period` | TabletKickCooldownPeriod | Integer, seconds | Minimum time interval between moves of the same tablet. | 600 ||
|| `spread_neighbours` | SpreadNeighbours | true/false | Starts tablets of the same scheme object (table or topic) on different nodes whenever possible. | true ||
|| `node_balance_strategy` | NodeBalanceStrategy | Enumeration | Strategy for selecting the node from which tablets are moved during autobalancing. Valid values:

- `HIVE_NODE_BALANCE_STRATEGY_WEIGHTED_RANDOM`: Weighted random selection based on resource usage.
- `HIVE_NODE_BALANCE_STRATEGY_HEAVIEST`: Selects the node with the highest resource usage.
- `HIVE_NODE_BALANCE_STRATEGY_RANDOM`: Selects a random node.

| `HIVE_NODE_BALANCE_STRATEGY_HEAVIEST` ||
|| `tablet_balance_strategy` | TabletBalanceStrategy | Enumeration | Strategy for selecting a tablet to move during autobalancing. Valid values:

- `HIVE_TABLET_BALANCE_STRATEGY_WEIGHTED_RANDOM`: Weighted random selection based on resource usage.
- `HIVE_TABLET_BALANCE_STRATEGY_HEAVIEST`: Selects the tablet with the highest resource usage.
- `HIVE_TABLET_BALANCE_STRATEGY_RANDOM`: Selects a random tablet.

| `HIVE_TABLET_BALANCE_STRATEGY_WEIGHTED_RANDOM` ||
|| `min_period_between_balance` | MinPeriodBetweenBalance | Floating-point number, seconds | Minimum time interval between two autobalancing iterations. Does not apply to emergency balancing. | 0.2 ||
|| `balancer_inflight` | BalancerInflight | Integer | Number of tablets being restarted concurrently during autobalancing. Does not apply to emergency balancing. | 1 ||
|| `max_movements_on_auto_balancer` | MaxMovementsOnAutoBalancer | Integer | Number of tablet moves per autobalancing iteration. Does not apply to emergency balancing. | 1 ||
|| `continue_auto_balancer` | ContinueAutoBalancer | true/false | When enabled, starts the next balancing iteration without waiting for `resource_change_reaction_period` to end. | true ||
|| `min_period_between_emergency_balance` | MinPeriodBetweenEmergencyBalance | Floating-point number, seconds | Same as `min_period_between_balance`, but for emergency balancing. | 0.1 ||
|| `emergency_balancer_inflight` | EmergencyBalancerInfligh | Integer | Same as `balancer_inflight`, but for emergency balancing. | 1 ||
|| `max_movements_on_emergency_balancer` | MaxMovementsOnEmergencyBalancer | Integer | Same as `max_movements_on_auto_balancer`, but for emergency balancing. | 2 ||
|| `continue_emergency_balancer` | ContinueEmergencyBalancer | true/false | Same as `continue_auto_balancer`, but for emergency balancing. | true ||
|| `check_move_expediency` | CheckMoveExpediency | true/false | Checks whether tablet moves are worthwhile. If autobalancing causes elevated Hive CPU consumption, you can disable this option. | true ||
|| `object_imbalance_to_balance` | ObjectImbalanceToBalance | Floating-point number | Threshold for the [imbalance metric of tablets that belong to the same object](../../contributor/hive.md#imbalance). | 0.02 ||
|| `less_system_tablets_moves` | LessSystemTabletMoves | true/false | Minimizes moving system tablets during autobalancing. | true ||
|| `balancer_ignore_tablet_types` | BalancerIgnoreTabletTypes | List of tablet types. When set through the Hive UI, values are separated by semicolons. | Tablet types excluded from autobalancing. | Empty list ||
|# {wide-content}

### Examples

This configuration file disables all types of tablet autobalancing between nodes.

```yaml
hive_config:
  min_cpuscatter_to_balance: 1.0
  min_memory_scatter_to_balance: 1.0
  min_network_scatter_to_balance: 1.0
  min_counter_scatter_to_balance: 1.0
  max_node_usage_to_kick: 3.0
  object_imbalance_to_balance: 1.0
```

This configuration file disables all types of autobalancing between nodes for tablets involved in transaction distribution, namely [coordinators](../../concepts/glossary.md#coordinator) and [mediators](../../concepts/glossary.md#mediator). Exact tablet type names are listed in the [glossary](../../concepts/glossary.md#tablet-types).


```yaml
hive_config:
  balancer_ignore_tablet_types:
    - Coordinator
    - Mediator
```

When using the Hive UI, enter `Coordinator;Mediator` in the BalancerIgnoreTabletTypes input field to get the same effect.

## Compute Resource Usage Metrics Collection Options {#metrics}

Hive collects [compute resource usage metrics](../../contributor/hive.md#resources) from each node - CPU time, RAM, and network - both for the node as a whole and per tablet. These settings control the collection, normalization, and aggregation of these metrics.

#|
|| Configuration parameter name | Hive web-viewer parameter name | Format | Description | Default value ||
|| `max_resource_cpu` | MaxResourceCPU | Integer, microseconds | Maximum CPU usage on a node per second. This default value is used only if the node does not provide the value when registering in Hive. | 10000000 ||
|| `max_resource_memory` | MaxResourceMemory | Integer, bytes | Maximum memory usage on a node. This default value is used only if the node does not provide the value when registering in Hive. | 512000000000 ||
|| `max_resource_network` | MaxResourceNetwork | Integer, bytes per second | Maximum bandwidth usage on a node. This default value is used only if the node does not provide the value when registering in Hive. | 1000000000 ||
|| `max_resource_counter` | MaxResourceCounter | Integer | Maximum usage of the virtual Counter resource on a node. | 100000000 ||
|| `metrics_window_size` | MetricsWindowSize | Integer, milliseconds | Window size for aggregating tablet resource usage metrics. | 60000 ||
|| `resource_overcommitment` | ResourceOvercommitment | Floating-point number | Node resource overcommitment factor. | 3.0 ||
|| `pools_to_monitor_for_usage` | — | Pool names separated by commas | Actor system pools whose consumption is included when calculating node resource usage. | System,User,IC ||
|# {wide-content}

## Channel Distribution Across Storage Groups Options {#storage}

This section lists options related to distributing tablet [channels](../../concepts/glossary.md#channel) across [storage groups](../../concepts/glossary.md#storage-group): taking different metrics into account, selecting groups, and autobalancing channels across groups.


{% note info %}

This table contains advanced settings that do not need to be changed in most cases.

{% endnote %}


#|
|| Configuration parameter name | Hive web-viewer parameter name | Format | Description | Default value ||
|| `default_unit_iops` | DefaultUnitIOPS | Integer | Default IOPS value for one channel. | 1 ||
|| `default_unit_throughput` | DefaultUnitThroughput | Integer, bytes per second | Default throughput consumption by one channel. | 1000 ||
|| `default_unit_size` | DefaultUnitSize | Integer, bytes | Default disk space consumption by one channel. | 100000000 ||
|| `storage_overcommit` | StorageOvercommit | Floating-point number | Storage group resource overcommitment factor. | 1.0 ||
|| `storage_balance_strategy` | StorageBalanceStrategy | Enumeration | Selects the parameter used to distribute tablet channels across storage groups. Valid values:

- `HIVE_STORAGE_BALANCE_STRATEGY_IOPS`: Only IOPS is taken into account.
- `HIVE_STORAGE_BALANCE_STRATEGY_THROUGHPUT`: Only throughput consumption is taken into account.
- `HIVE_STORAGE_BALANCE_STRATEGY_SIZE`: Only used space is taken into account.
- `HIVE_STORAGE_BALANCE_STRATEGY_AUTO`: Uses whichever of the parameters above has the highest consumption.

| `HIVE_STORAGE_BALANCE_STRATEGY_SIZE` ||
|| `storage_safe_mode` | StorageSafeMode | true/false | Checks whether storage group resource consumption exceeds maximum values. | true ||
|| `storage_select_strategy` | StorageSelectStrategy | Enumeration | Strategy for selecting a storage group for a tablet channel. Valid values:

- `HIVE_STORAGE_SELECT_STRATEGY_WEIGHTED_RANDOM`: Weighted random selection based on resource usage.
- `HIVE_STORAGE_SELECT_STRATEGY_EXACT_MIN`: Selects the group with the lowest resource usage.
- `HIVE_STORAGE_SELECT_STRATEGY_RANDOM_MIN_7P`: Selects a random group from the 7% of groups with the lowest resource usage.
- `HIVE_STORAGE_SELECT_STRATEGY_RANDOM`: Selects a random group.
- `HIVE_STORAGE_SELECT_STRATEGY_ROUND_ROBIN`: Selects a group within the storage pool using the [round-robin](https://en.wikipedia.org/wiki/Round-robin_scheduling) principle.
| `HIVE_STORAGE_SELECT_STRATEGY_WEIGHTED_RANDOM` ||
|| `min_period_between_reassign` | MinPeriodBetweenReassign | Integer, seconds | Minimum time interval between storage group reassignments for channels of the same tablet. | 300 ||
|| `storage_pool_fresh_period` | StoragePoolFreshPeriod | Integer, milliseconds | Frequency for updating storage pool information. | 60000 ||
|| `space_usage_penalty_threshold` | SpaceUsagePenaltyThreshold | Floating-point number | Minimum ratio of free space in the target group to free space in the source group at which the target group is penalized by applying a multiplicative penalty to its weight when moving a channel. | 1.1 ||
|| `space_usage_penalty` | SpaceUsagePenalty | Floating-point number | Penalty factor for the behavior described above. | 0.2 ||
|| `channel_balance_strategy` | ChannelBalanceStrategy | Enumeration | Strategy for selecting a channel to reassign during channel balancing. Valid values:

- `HIVE_CHANNEL_BALANCE_STRATEGY_WEIGHTED_RANDOM`: Weighted random selection based on resource usage.
- `HIVE_CHANNEL_BALANCE_STRATEGY_HEAVIEST`: Selects the channel with the highest resource usage.
- `HIVE_CHANNEL_BALANCE_STRATEGY_RANDOM`: Selects a random channel.

| `HIVE_CHANNEL_BALANCE_STRATEGY_WEIGHTED_RANDOM` ||
|| `max_channel_history_size` |  MaxChannelHistorySize | Integer | Maximum channel history size. | 200 ||
|| `storage_info_refresh_frequency` | StorageInfoRefreshFrequency | Integer, milliseconds | Frequency for updating storage pool information. | 600000 ||
|| `min_storage_scatter_to_balance` | MinStorageScatterToBalance | Floating-point number | Scatter metric threshold for storage groups. | 999 ||
|| `min_group_usage_to_balance` | MinGroupUsageToBalance | Floating-point number | Storage group resource usage threshold below which balancing does not start. | 0.1 ||
|| `storage_balancer_inflight` | StorageBalancerInflight | Integer | Number of tablets being restarted concurrently during channel balancing. | 1 ||
|# {wide-content}

## Restart Tracking Options {#restarts}

Hive tracks how often different nodes and tablets restart to identify problematic ones. These options let you configure exactly which tablets or nodes are considered problematic and how this affects them. Based on these statistics, nodes and tablets are included in the [HealthCheck API](../ydb-sdk/health-check-api.md) report.

### Tablet Restart Tracking Options

#|
|| Configuration parameter name | Hive web-viewer parameter name | Format | Description | Default value ||
|| `tablet_restart_watch_period` | — | Integer, seconds | Window size for collecting statistics on the number of tablet restarts. **This period is used only for statistics passed to HealthCheck.** | 3600 ||
|| `tablet_restarts_period` | — | Integer, milliseconds | Window size for counting tablet restarts when penalizing startup of problematic tablets. | 1000 ||
|| `tablet_restarts_max_count` | — | Integer | Number of restarts in the `tablet_restarts_period` window after which a penalty is applied. | 2 ||
|| `postopone_start_period` | — | Integer, milliseconds | Frequency of attempts to start problematic tablets. | 1000 ||
|# {wide-content}

### Node Restart Tracking Options

#|
|| Configuration parameter name | Hive web-viewer parameter name | Format | Description | Default value ||
|| `node_restart_watch_period` | — | Integer, seconds | Window size for collecting statistics on the number of node restarts. | 3600 ||
|| `node_restarts_for_penalty` | NodeRestartsForPenalty | Integer | Number of restarts in the `node_restart_watch_period` window after which nodes get a lower priority. | 3 ||
|# {wide-content}


## Miscellaneous {#misc}

This section lists additional Hive settings.


{% note info %}

This table contains advanced settings that do not need to be changed in most cases.

{% endnote %}


#|
|| Configuration parameter name | Hive web-viewer parameter name | Format | Description | Default value ||
|| `drain_inflight` | DrainInflight | Integer | Number of tablets being restarted concurrently during graceful movement of all tablets from one node (drain). | 10 ||
|| `request_sequence_size` | — | Integer | Number of tablet IDs that a database Hive requests from the root Hive at a time. | 1000 ||
|| `min_request_sequence_size` | — | Integer | Minimum number of tablet IDs that the root Hive allocates for a database Hive at a time. | 1000 ||
|| `max_request_sequence_size` | — | Integer | Maximum number of tablet IDs allocated for a database Hive at a time. | 1000000 ||
|| `node_delete_period` | — | Integer, seconds | Inactivity period after which a node is deleted from the Hive database. | 3600 ||
|| `warm_up_enabled` | WarmUpEnabled | true/false | When enabled, Hive waits for all nodes to connect on database startup before starting tablets. When disabled, all tablets can be started on the first connected node. | true ||
|| `warm_up_boot_waiting_period` | MaxWarmUpBootWaitingPeriod | Integer, milliseconds | Time to wait for all known nodes to start on database startup. | 30000 ||
|| `max_warm_up_period` | MaxWarmUpPeriod | Integer, seconds | Maximum time to wait for nodes to start on database startup. | 600 ||
|| `enable_destroy_operations` | — | true/false | Whether destructive manual operations are allowed. | false ||
|| `max_pings_in_flight` | — | Integer | Maximum number of connections to nodes being established in parallel. | 1000 ||
|| `cut_history_deny_list` | — | Comma-separated list of tablet types | List of tablet types for which the history cleanup operation is ignored. | ColumnShard,KeyValue,PersQueue,BlobDepot ||
|| `cut_history_allow_list` | — | Comma-separated list of tablet types | List of tablet types for which the history cleanup operation is allowed. | DataShard ||
|| `scale_recommendation_refresh_frequency` | ScaleRecommendationRefreshFrequency | Integer, milliseconds | How often the recommended number of compute nodes is updated. | 60000 ||
|| `scale_out_window_size` | ScaleOutWindowSize | Integer | Number of buckets used to decide whether to recommend increasing the number of compute nodes. | 15 ||
|| `scale_in_window_size` | ScaleInWindowSize | Integer | Number of buckets used to decide whether to recommend decreasing the number of compute nodes. | 5 ||
|| `target_tracking_cpumargin` | TargetTrackingCPUMargin | Floating-point number | Allowed deviation from the target CPU utilization value during autoscaling. | 0.1 ||
|| `dry_run_target_tracking_cpu` | DryRunTargetTrackingCPU | Floating-point number | Target CPU utilization value used to check how autoscaling would work. | 0 ||
|# {wide-content}
