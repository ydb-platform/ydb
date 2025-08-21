# Working with SelfHeal State Storage

While a clusters are running, entire nodes that {{ ydb-short-name }} runs on can fail.

SelfHeal ensures a cluster's continuous performance and fault tolerance if malfunctioning nodes cannot be repaired quickly.
It also automatically distributes the load to more replicas when new nodes are added to the cluster.

SelfHeal can:

* Detect faulty system nodes;
* detect an increase in load and redistribute it to more replicas;
* move StateStorage, Board, and SchemeBoard replicas to other nodes or add new replicas.

State storage SelfHeal is enabled by default.

{{ ydb-short-name }} component responsible for State Storage SelfHeal is called "Sentinel".

## Enabling and disabling State Storage SelfHeal and options {#on-off}

You can enable and disable SelfHeal using configuration.
Both CMS Sentinel and distconf must be activated to work.

```yaml
config:
  	self_management_config:
    	enabled: true # When you enable distconf, it generates a new configuration and sends it to the nodes
  	cms_config:
		sentinel_config:
			enable: true # Enabling Sentinel, which monitors node availability and issues a self-heal command in distconf
			update_state_interval: 60000000 # Node status polling frequency. Default is 1 minute
			state_storage_self_heal_config:
				enable: true # Enabling self-heal state storage
				node_good_state_limit: 10 # The number of polls during which the node must be in good condition (available) in order to be used in the configuration
				node_pretty_good_state_limit: 7 # The number of polls during which the node must be in good condition in order to use it in the configuration, but the SelfHeal command is not issued in distconf
				node_bad_state_limit: 10 # The number of cycles during which the node must be unavailable in order to issue the self-heal command in distconf
				wait_for_config_step: 60000000 # The period between configuration change steps. The default value is 1 minute.
                relax_time: 360000000 # Minimum period between SelfHeal. commands. Default is 10 minutes
				pileup_replicas: false # StateStorage, Board, SchemeBoard replicas should be located on the same node. If the cluster can be rolled back to the old V1 config version, this will result in a compatible state
```

## How it works

BS_CONTROLLER keeps in touch with all the NodeWarden's of the storage nodes that are running in the cluster.
Each node registers with BSC when it starts and maintains a persistent connection, which is recorded when it is disconnected.
The status of each node is collected, including how long it has been unavailable, which is used to determine whether to initiate the self-healing process.
Self-healing generates a recommended configuration for the cluster based on the availability of the nodes, and if it differs from the current configuration, it is applied.
The change in the cluster's node composition is also monitored, and one replica is added to each ring for every 1,000 nodes. This allows for automatic load balancing.
