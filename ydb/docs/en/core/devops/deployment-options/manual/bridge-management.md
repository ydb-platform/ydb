## Managing a cluster in bridge mode

{% include [feature_enterprise.md](../../../_includes/feature_enterprise.md) %}

Below are typical operations for a cluster in [bridge mode](../../../concepts/bridge.md) using the [corresponding {{ ydb-short-name }} CLI commands](../../../reference/ydb-cli/commands/bridge/index.md).

### View current state {#list}

Shows the current state of each pile configured on the {{ ydb-short-name }} cluster.

```bash
{{ ydb-cli }} admin cluster bridge list
```

Example output:

```bash
pile-a: PRIMARY
pile-b: SYNCHRONIZED
```

### Planned `PRIMARY` change (switchover) {#switchover}

If planned maintenance is scheduled in the foreseeable future in the data center or on the equipment where the current `PRIMARY` pile is running, it is recommended to switch the cluster to use another pile as `PRIMARY` in advance. Select another pile in the `SYNCHRONIZED` state to switch it to the `PRIMARY` state with the following command:

```bash
{{ ydb-cli }} admin cluster bridge switchover --new-primary <pile>
```

The switchover is performed smoothly: roles go through `PRIMARY/PROMOTED` and end in the `SYNCHRONIZED/PRIMARY` state.

### Planned pile disconnection (takedown) {#takedown}

If planned maintenance will make one of the pile unavailable, it must be taken out of the cluster before starting using the following command:

```bash
{{ ydb-cli }} admin cluster bridge takedown --pile <pile>
# if disconnecting the current PRIMARY:
{{ ydb-cli }} admin cluster bridge takedown --pile <current-primary> --new-primary <synchronized-pile>
```

When the operation is performed, the pile is transitioned to `SUSPENDED`, then to `DISCONNECTED`; further cluster operations are performed without the disconnected pile.

If disconnecting the current `PRIMARY` and it was not possible to [change it in advance](#switchover), these operations can be combined by specifying the new `PRIMARY` in the `--new-primary` argument, which must be in the `SYNCHRONIZED` state.

```bash
{{ ydb-cli }} admin cluster bridge takedown --pile <pile>
# if disconnecting the current PRIMARY:
{{ ydb-cli }} admin cluster bridge takedown --pile <current-primary> --new-primary <synchronized-pile>
```

{% note warning %}

Before starting planned maintenance, always verify using the [list](#list) command that the pile disconnection operation has completed successfully and all pile are in the expected state.

{% endnote %}

### Emergency disconnection of unavailable pile (failover) {#failover}

Since synchronous replication operates between pile, when one of them unexpectedly fails, cluster operation stops by default, and a decision must be made whether to continue cluster operation without this pile. This decision can be made by a person (for example, an on-call DevOps engineer) or by automation external to the {{ ydb-short-name }} cluster.

If the decision is to continue cluster operation, run the following command:

```bash
{{ ydb-cli }} admin cluster bridge failover --pile <unavailable-pile>
```

If the current `PRIMARY` is unavailable, you must add the `--new-primary` parameter with the name of a pile in the `SYNCHRONIZED` state. If the parameter is not specified or is specified incorrectly, the command will fail with an error without any changes to the cluster.

```bash
{{ ydb-cli }} admin cluster bridge failover --pile <unavailable-pile>
# if the current PRIMARY is unavailable:
{{ ydb-cli }} admin cluster bridge failover --pile <unavailable-primary> --new-primary <synchronized-pile>
```

The unavailable pile will be transitioned to the `DISCONNECTED` state, and when a new `PRIMARY` is specified, that role will be switched. If other pile are in states other than `SYNCHRONIZED`, emergency disconnection can also be performed. Valid transitions depend on the current state pair and are shown on the [state diagram](../../../concepts/bridge.md#pile-states) and in the [transition table](../../../concepts/bridge.md#transitions-between-states).

### Return pile to the cluster (rejoin) {#rejoin}

After planned maintenance is complete or the causes of the failure have been resolved, previously disconnected pile must be explicitly brought back into operation with the following command:

```bash
{{ ydb-cli }} admin cluster bridge rejoin --pile <pile>
```

Immediately after the operation starts, the pile transitions to the `NOT_SYNCHRONIZED` state and a background data synchronization process starts; when synchronization completes, the pile automatically becomes `SYNCHRONIZED`. After waiting for this state, you can [switch the `PRIMARY` role to this pile](#switchover) if needed.
