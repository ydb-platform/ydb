# State Storage Reconfiguration

If you need to change the configuration of [State Storage](../../../reference/configuration/index.md#domains-state) on the {{ ydb-short-name }} cluster due to prolonged failure of the nodes on which the replicas are running, or due to an increase in the size of the cluster and an increase in the load.

{% include [warning-configuration-error](../configuration-v1/_includes/warning-configuration-error.md) %}

When using the V2 Configuration, State Storage is configured automatically. However, during cluster operation, there may be long node failures or increased load, which may require configuration changes.

If you need to change the configuration on a cluster, you can do so manually using distconf.

## GetStateStorageConfig

Returns the current configuration for StateStorage, Board, and SchemeBoard
	Recommended: true will return the recommended configuration for this cluster, which can be compared with the current configuration to determine whether it should be applied
	PileupReplicas - create a recommended configuration that allows you to roll back to V1

Request example
```shell
curl -ks http://{host_name}:8765/actors/nodewarden?page=distconf -X POST -H 'Content-Type: application/json' -d '{"GetStateStorageConfig": {"Recommended": true}}' | jq
```

## SelfHealStateStorage

The command starts the translation of the configuration to the recommended one. It is executed in 4 steps, and a WaitForConfigStep pause is made between the steps to allow the new configuration to spread to the nodes, apply, and create new replicas with data.
Example:
```shell
curl -ks http://{host_name}:8765/actors/nodewarden?page=distconf -X POST -H 'Content-Type: application/json' -d '
{
	"SelfHealStateStorage": {
		"ForceHeal": true
	}
}' | jq
```

**WaitForConfigStep** - The period between configuration change steps. The default value is 60 seconds.
**ForceHeal** - If distconf cannot generate a config from only good nodes, it will use non-working nodes. This reduces the fault tolerance. You can apply this config by setting this option to true.
**PileupReplicas** - create a recommended configuration that allows you to roll back to V1

## ReconfigStateStorage

The command applies the specified configuration. The configuration StateStorageConfig, StateStorageBoardConfig, SchemeBoardConfig is specified separately.
You cannot change the configuration directly, as this will cause the cluster to fail. Therefore, changes are made by adding new ring groups and removing old ones.
However, you can only add or remove groups of rings with the WriteOnly: true flag enabled.
The first ring group in the list must be WriteOnly: false or not have this flag. This condition ensures that there will always be at least one fully operational group of rings.
It is recommended to wait a time (1 min) between the application of the new configuration until the new configuration is distributed to the cluster nodes, new replicas are created and filled.
Using SchemeBoardConfig as an example (for the rest it is the same and can be performed simultaneously)

**Step 1**
In the first step, the first ring group must match the current one. Add a new ring group that matches the target configuration and mark it as WriteOnly: true.
```shell
curl -ks http://{host_name}:8765/actors/nodewarden?page=distconf -X POST -H 'Content-Type: application/json' -d '
{
	ReconfigStateStorage: {
		SchemeBoardConfig: {
			RingGroups: [
				{ NToSelect: 5, Node: [1,2,3,4,5,6,7,8] },
				{ NToSelect: 5, Node: [10,20,30,40,5,6,7,8], WriteOnly: true }
			]
		}
	}
}
```

**Step 2**
Remove the WriteOnly flag.
```shell
curl -ks http://{host_name}:8765/actors/nodewarden?page=distconf -X POST -H 'Content-Type: application/json' -d '
{
	ReconfigStateStorage: {
		SchemeBoardConfig: {
			RingGroups: [
				{ NToSelect: 5, Node: [1,2,3,4,5,6,7,8] },
				{ NToSelect: 5, Node: [10,20,30,40,5,6,7,8] }
			]
		}
	}
}
```

**Step 3**
Make the new ring group the main one. Prepare the old configuration for deletion by setting the WriteOnly: true flag
```shell
curl -ks http://{host_name}:8765/actors/nodewarden?page=distconf -X POST -H 'Content-Type: application/json' -d '
{
	ReconfigStateStorage: {
		SchemeBoardConfig: {
			RingGroups: [
				{ NToSelect: 5, Node: [10,20,30,40,5,6,7,8] },
				{ NToSelect: 5, Node: [1,2,3,4,5,6,7,8], WriteOnly: true }
			]
		}
	}
}
```

**Step 4**
One new configuration remains.
```shell
curl -ks http://{host_name}:8765/actors/nodewarden?page=distconf -X POST -H 'Content-Type: application/json' -d '
{
	ReconfigStateStorage: {
		SchemeBoardConfig: {
			RingGroups: [
				{ NToSelect: 5, Node: [10,20,30,40,5,6,7,8] }
			]
		}
	}
}
```
