## Managing a cluster in bridge mode

{% include [feature_enterprise.md](../../../_includes/feature_enterprise.md) %}

Below are typical operations for converting a cluster to [bridge mode](../../../concepts/bridge.md) and managing it using the [corresponding {{ ydb-short-name }} CLI commands](../../../reference/ydb-cli/commands/bridge/index.md).

### Convert an ordinary cluster to a single-pile bridge {#convert}

Conversion enables bridge mode for system storage and, by default, for all existing database storage pools. It preserves existing data and is irreversible.

1. Make sure the cluster uses configuration V2 with `self_management_config.enabled: true` and that every static and dynamic node runs a build that supports bridge conversion. For configuration V1, first complete the [migration to V2](../../configuration-management/migration/migration-to-v2.md).

2. Fetch the current configuration through an available node:

    ```bash
    {{ ydb-cli }} -e grpc://<node>:2135 admin cluster config fetch > config.yaml
    ```

3. Add exactly one pile to `config.bridge_config` in `config.yaml`, preserving `metadata` and the other settings:

    ```yaml
    config:
      bridge_config:
        piles:
          - name: pile-a
    ```

    To leave existing database pools ordinary, also set `bridge_config.enable_non_uniform_storage: true`. Do not set this option for default conversion; it can be enabled after conversion completes if mixed storage modes are required.

4. Replace the configuration and perform a rolling restart of all static and dynamic nodes. Every connected node must run with the new `bridge_config` before conversion starts:

    ```bash
    {{ ydb-cli }} -e grpc://<node>:2135 -y admin cluster config replace -f config.yaml
    ```

5. Select the required scenario and run only one of these commands.

    For the standard conversion of system storage and all existing database pools to bridge mode, run:

    ```bash
    {{ ydb-cli }} -y admin cluster bridge convert
    ```

    To convert only system storage while leaving existing database pools ordinary, run this command instead:

    ```bash
    {{ ydb-cli }} -y admin cluster bridge convert --skip-storage-pool-conversion
    ```

    The second scenario requires `bridge_config.enable_non_uniform_storage: true` from step 3. Existing databases can later be [switched to bridged storage](#database-mode) individually.

6. Use [bridge list](#list) to wait until conversion completes. While the output contains `Conversion from non-bridged cluster is in progress, piles can't be added yet`, do not change the pile list. After completion, the single pile is `PRIMARY`.

Conversion puts all existing nodes into one pile; it does not create a separate pile for each existing data center. For command details, see [admin cluster bridge convert](../../../reference/ydb-cli/commands/bridge/convert.md).

### Add a pile {#add-pile}

Adding a pile allocates its storage and synchronizes bridged data without stopping the cluster.

1. If you ran [ordinary-cluster conversion](#convert), wait for it to complete. If the cluster initially ran in bridge mode or conversion has already completed, no additional wait is required. In either case, check the cluster state before adding a pile:

    ```bash
    {{ ydb-cli }} admin cluster bridge list
    ```

    Wait until the `Conversion from non-bridged cluster is in progress, piles can't be added yet` message and any pending `ADD` or `REMOVE` operations disappear.

2. If every existing node already has its pile name assigned, skip this step. Otherwise, assign the names before adding the new pile to `bridge_config`.

    Fetch the current configuration:

    ```bash
    {{ ydb-cli }} -e grpc://<node>:2135 admin cluster config fetch > config.yaml
    ```

    In `config.hosts`, find every existing static node's entry and add `location.bridge_pile_name` with the name of its current pile. Edit the existing entries instead of duplicating them, and preserve the other `location` fields:

    ```yaml
    config:
      hosts:
        - host: node-a1
          host_config_id: 1
          location:
            data_center: dc-a
            rack: rack-a1
            bridge_pile_name: pile-a
        # ... update all other existing static nodes in the same way
    ```

    For every existing dynamic node, specify its current pile in the `ydbd server` command-line option, for example `--bridge-pile-name pile-a`. Dynamic nodes are not listed in `hosts`.

    Apply the static-node configuration change:

    ```bash
    {{ ydb-cli }} -e grpc://<node>:2135 -y admin cluster config replace -f config.yaml
    ```

    Perform a [rolling restart](../../../maintenance/manual/node_restarting.md) of every static and dynamic node whose settings changed. Do not add the new pile to `bridge_config` until the restart is complete.

3. Fetch the current configuration again to use its current version:

    ```bash
    {{ ydb-cli }} -e grpc://<node>:2135 admin cluster config fetch > config.yaml
    ```

    Add the new pile to `bridge_config.piles` and its static nodes to `hosts`. Set `location.bridge_pile_name` for every new node as described in the [hosts configuration](../../../reference/configuration/hosts.md#hosts-bridge). If an existing `host_configs` entry describes the new nodes' disks, reuse its `host_config_id`; otherwise, add a new host configuration.

    For example, the following `config` fragment defines `pile-a` and `pile-b` with the same disk configuration. Only one static node per pile is shown for brevity. Add all the required nodes and preserve `metadata`, existing entries, and the other settings in `config.yaml`:

    ```yaml
    config:
      bridge_config:
        piles:
          - name: pile-a
          - name: pile-b

      host_configs:
        - host_config_id: 1
          drive:
            - path: /dev/disk/by-partlabel/ydb_disk_ssd_01
              type: SSD

      hosts:
        - host: node-a1
          host_config_id: 1
          location:
            data_center: dc-a
            rack: rack-a1
            bridge_pile_name: pile-a
        # ... other static nodes in pile-a
        - host: node-b1
          host_config_id: 1
          location:
            data_center: dc-b
            rack: rack-b1
            bridge_pile_name: pile-b
        # ... other static nodes in pile-b
    ```

    If `system_tablets` explicitly lists node IDs, include nodes from the new pile so system tablets can run there after a switchover or failover.

4. Apply the updated configuration through an available node:

    ```bash
    {{ ydb-cli }} -e grpc://<node>:2135 -y admin cluster config replace -f config.yaml
    ```

    Start the new pile's static nodes and check its state:

    ```bash
    {{ ydb-cli }} -e grpc://<node>:2135 admin cluster bridge list
    ```

    The nodes remain idle while the command output shows the pile as `INACTIVE`.

5. Provision storage for the pile:

    ```bash
    {{ ydb-cli }} -y admin cluster bridge provision-pile --pile pile-b
    ```

    This starts an `ADD` operation that creates the new pile's parts of static and bridged storage groups. Ordinary groups are not changed. Use [bridge list](#list) to wait until the pending operation clears and the pile becomes `DISCONNECTED`.

6. Join the provisioned pile to the cluster:

    ```bash
    {{ ydb-cli }} -y admin cluster bridge join-pile --pile pile-b
    ```

    The pile enters `NOT_SYNCHRONIZED`, synchronizes in the background, and then automatically becomes `SYNCHRONIZED`. Wait for this state with [bridge list](#list).

7. Start the new pile's dynamic nodes by adding the `--bridge-pile-name pile-b` option to the `ydbd server` command along with the other startup options.

For command details, see [provision-pile](../../../reference/ydb-cli/commands/bridge/provision-pile.md) and [join-pile](../../../reference/ydb-cli/commands/bridge/rejoin.md).

### Remove a pile {#remove-pile}

Removing a pile permanently deletes its storage-group parts and all data in them.

1. If the only retained copies of any database are in the pile being removed, for example after switching to ordinary storage with `--retain-pile pile-b`, first [switch that database to bridged storage](#database-mode). Pile states may remain unchanged. Before switching the database back to ordinary storage with `--retain-pile pile-a`, wait until every storage group being converted has a `SYNCED` part in `pile-a`, as described in [Verifying a database mode change](#verify-database-mode). After the switch to ordinary storage, redundant group parts enter `BLOCKS` and are deleted in the background.

2. If `pile-b` is `PRIMARY`, first perform a [switchover](#switchover) to another synchronized pile, such as `pile-a`. Then [take down](#takedown) the pile being removed and check its state:

    ```bash
    # Only if pile-b is the current PRIMARY:
    {{ ydb-cli }} admin cluster bridge switchover --new-primary pile-a

    {{ ydb-cli }} admin cluster bridge takedown --pile pile-b
    {{ ydb-cli }} admin cluster bridge list
    ```

    Repeat `bridge list` until `pile-b` becomes `DISCONNECTED`, then stop all its nodes.

3. Destroy the pile:

    ```bash
    {{ ydb-cli }} -y admin cluster bridge destroy-pile --pile pile-b
    ```

    Before starting the `REMOVE` operation, {{ ydb-short-name }} checks that no ordinary storage group has VDisks in this pile and no bridged group depends solely on its part there. If this check fails, the command returns an error with the group ID and does not start removal. Resolve the cause of the error and rerun the command.

    After the check succeeds, the command starts a `REMOVE` operation. Use [bridge list](#list) to wait until the pending operation clears and the pile becomes `INACTIVE`. Without the global `-y` option, the command asks for confirmation before deleting data.

4. After the pile becomes `INACTIVE`, remove it and its hosts from `bridge_config`, `hosts`, and `host_configs`, and remove its node IDs from any explicit `system_tablets` lists.

For command details, see [admin cluster bridge destroy-pile](../../../reference/ydb-cli/commands/bridge/destroy-pile.md).

### Switch a database between bridged and ordinary storage {#database-mode}

A bridge cluster can contain databases using bridged storage and databases using ordinary storage when `bridge_config.enable_non_uniform_storage: true` is set. Conversion must be complete, and no pile addition or removal can be in progress. If you change this configuration option, restart all static and dynamic nodes before continuing.

The following commands change all storage pools of the selected database without deleting its existing data:

```bash
# Replicate the database across all provisioned piles:
{{ ydb-cli }} -y admin cluster bridge set-database-mode bridged --target-database /Root/db1

# Retain copies in pile-a and delete redundant copies in other piles in the background:
{{ ydb-cli }} -y admin cluster bridge set-database-mode ordinary --target-database /Root/db1 --retain-pile pile-a
```

`--target-database` must be the absolute path of the database and is independent of the global `--database` connection option. For a serverless database, specify its shared database; the change applies to that shared database and all its serverless databases.

When switching to ordinary storage, `--retain-pile` selects the pile whose group parts must be retained. It does not disconnect the pile or change its role: you can specify `PRIMARY` or another pile in the `SYNCHRONIZED` state. The selected pile must already contain synchronized parts of every group being converted. If this option is omitted, {{ ydb-short-name }} selects retained copies automatically, potentially in different piles for different groups.

When switching to bridged storage, every existing group must reside within a single pile; a group spanning several piles cannot be converted directly.

The command changes all database pool modes atomically: if validation fails, no pool mode is changed. After the mode change succeeds, background synchronization or cleanup may still be in progress. For command details, see [admin cluster bridge set-database-mode](../../../reference/ydb-cli/commands/bridge/set-database-mode.md).

### Verify a database mode change {#verify-database-mode}

Successful completion of `set-database-mode` confirms the atomic mode change of all database pools, not the completion of background synchronization or cleanup. The `bridge list` command does not show these operations on the database's storage groups; pile states may remain unchanged.

For diagnostics, use the [`.sys/ds_groups` system view](../../../devops/observability/system-views.md#ds_groups). First, find the database's storage pool IDs:

```bash
{{ ydb-cli }} sql -s '
SELECT BoxId, StoragePoolId, Name
FROM `/Root/.sys/ds_storage_pools`
WHERE Name LIKE "/Root/db1:%";
'
```

Then inspect the groups in each pool:

```bash
{{ ydb-cli }} sql -s '
SELECT
    GroupId,
    ProxyGroupId,
    BridgePileId,
    BridgeSyncStage,
    BridgeDataSyncProgress,
    BridgeSyncRunning,
    BridgeSyncLastError
FROM `/Root/.sys/ds_groups`
WHERE BoxId = <box-id>
  AND StoragePoolId = <storage-pool-id>
ORDER BY GroupId;
'
```

Here, `<box-id>` and `<storage-pool-id>` are the IDs returned by the first query. Inspect physical group-part rows with `BridgePileId` set; synchronization fields can be empty for logical proxy groups. `ProxyGroupId` associates a physical part with its logical group. For an original inline part, this field can be empty while its `GroupId` matches the logical group ID.

Completion criteria depend on the selected mode:

- **Full transition to bridged storage:** every participating pile is `PRIMARY` or `SYNCHRONIZED`, and all its physical parts of every logical group are `SYNCED`.
- **Readiness to switch to ordinary storage with `--retain-pile`:** the selected pile is `PRIMARY` or `SYNCHRONIZED`, and its part in every logical group being converted is already `SYNCED`.
- **Cleanup after switching to ordinary storage:** redundant part rows first show `BLOCKS` and then disappear, leaving only the retained physical parts.

`BridgeSyncLastError` contains the most recent synchronization error. `BridgeDataSyncProgress` covers only the data-copy stage, not the entire conversion. Do not use `BridgeSyncRunning = false` as the sole completion indicator: synchronization may still be waiting to start.

### View current state {#list}

Shows the current state of each pile configured on the {{ ydb-short-name }} cluster, conversion progress, and pending pile operations.

```bash
{{ ydb-cli }} admin cluster bridge list
```

Example output:

```text
Pile pile-a: PRIMARY
Pile pile-b: SYNCHRONIZED
Generation: 4
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

If several piles fail at the same time, list all of them in one command by repeating `--pile`. For example, if `pile-a` and `pile-b` fail and `pile-c` is still `SYNCHRONIZED`, connect to a node in `pile-c` and run:

```bash
{{ ydb-cli }} admin cluster bridge failover --pile pile-a --pile pile-b --new-primary pile-c
```

This is one configuration update that disconnects both unavailable piles and selects the new `PRIMARY`. Disconnecting them one at a time may fail: while the second failed pile is still part of the configuration quorum, the first update cannot obtain that quorum. The remaining pile must still have the node and storage quorums required to apply the new configuration and serve requests.

Before running the command, ensure the failed piles are stopped or isolated so they cannot independently resume serving requests. Do not issue failover commands independently on opposite sides of a network partition; see [split-brain recovery](../../../concepts/bridge.md#split-brain). The command does not enable automatic failover or allow an unsynchronized pile to become `PRIMARY`. After the command succeeds, check the result with [list](#list). Recover each failed pile using [rejoin](#rejoin) after resolving the failure.

The unavailable pile will be transitioned to the `DISCONNECTED` state, and when a new `PRIMARY` is specified, that role will be switched. If other pile are in states other than `SYNCHRONIZED`, emergency disconnection can also be performed. Valid transitions depend on the current state pair and are shown on the [state diagram](../../../concepts/bridge.md#pile-states) and in the [transition table](../../../concepts/bridge.md#transitions-between-states).

{% note warning %}

If mandatory authentication is enabled on the cluster, login and password authentication cannot be used while the cluster is in an emergency state — use client certificate authentication instead. For details, see [{#T}](#emergency-auth).

{% endnote %}

### Return pile to the cluster (rejoin) {#rejoin}

After planned maintenance is complete or the causes of the failure have been resolved, previously disconnected pile must be explicitly brought back into operation with the following command:

```bash
{{ ydb-cli }} admin cluster bridge rejoin --pile <pile>
```

Immediately after the operation starts, the pile transitions to the `NOT_SYNCHRONIZED` state and a background data synchronization process starts; when synchronization completes, the pile automatically becomes `SYNCHRONIZED`. After waiting for this state, you can [switch the `PRIMARY` role to this pile](#switchover) if needed.

### Authentication specifics when the cluster is in an emergency state {#emergency-auth}

Bridge cluster management commands require cluster administrator privileges. If mandatory authentication is enabled ([`enforce_user_token_requirement: true`](../../../reference/configuration/security_config.md)), all authentication methods are available during normal cluster operation, including [login and password authentication](../../../security/authentication.md#static-credentials).

However, when a pile fails, the cluster suspends serving requests until [failover](#failover) is performed. In this state, login and password authentication cannot be used: validating credentials requires access to [SchemeShard](../../../concepts/glossary.md#scheme-shard), which is unavailable while the cluster is in an emergency state. A login attempt fails with the following error:

```text
SchemeShard is unreachable
```

The only authentication method available while the cluster is in an emergency state is [client certificate authentication](../../../reference/configuration/client_certificate_authorization.md). The client certificate is verified locally by the node that received the request, so this check does not depend on the availability of the cluster as a whole.

Client certificate authentication must be configured on the cluster in advance:

1. The [`client_certificate_authorization`](../../../reference/configuration/client_certificate_authorization.md) section of the cluster configuration must assign an administrative group [SID](../../../concepts/glossary.md#access-sid) to connections with a trusted client certificate:

    ```yaml
    client_certificate_authorization:
      request_client_certificate: true
      client_certificate_definitions:
        - member_groups: ["ADMINS"]
          subject_terms:
          - short_name: "O"
            values: ["YDB"]
    ```

2. This SID must be included in the `administration_allowed_sids` list of the [`security_config`](../../../reference/configuration/security_config.md) section:

    ```yaml
    security_config:
      enforce_user_token_requirement: true
      administration_allowed_sids:
      - "root"
      - "ADMINS"
    ```

If the cluster is deployed following the [deployment guide](initial-deployment/deployment-configuration-v2.md), these settings are already included in the configuration, and the `node.crt` and `node.key` files from the `~/CA/certs/` directory on any cluster node can be used as the client certificate.

To authenticate with a client certificate, specify it in the {{ ydb-short-name }} CLI global options `--client-cert-file` and `--client-cert-key-file`. Connect to a node of a healthy pile. Failover example:

```bash
{{ ydb-cli }} -e grpcs://<node.ydb.tech>:2135 \
    --ca-file ca.crt \
    --client-cert-file node.crt \
    --client-cert-key-file node.key \
    admin cluster bridge failover --pile <unavailable-pile> --new-primary <synchronized-pile>
```

where:

- `<node.ydb.tech>` — FQDN of a node in a healthy pile;
- `ca.crt` — certificate of the cluster's trusted certificate authority;
- `node.crt`, `node.key` — client certificate and its private key that satisfy the requirements of the `client_certificate_authorization` section.

{% note warning %}

Verify that client certificate authentication works in advance, before an emergency occurs. If client certificate authentication was not configured before the emergency, bringing the cluster out of the emergency state becomes significantly more complicated and requires manual intervention on the cluster nodes.

{% endnote %}
