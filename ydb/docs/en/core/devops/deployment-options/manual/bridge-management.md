# Managing a cluster in bridge mode

{% include [feature_enterprise.md](../../../_includes/feature_enterprise.md) %}

Bridge cluster management is a set of procedures for converting a cluster to [bridge mode](../../../concepts/bridge.md), changing its pile topology and storage pool modes, and recovering from pile failures. This article describes these procedures using [{{ ydb-short-name }} CLI commands](../../../reference/ydb-cli/commands/bridge/index.md) and {{ ydb-short-name }} DSTool.

## Common command usage {#common}

Run management commands through any available cluster node. Add the endpoint to the command as follows:

```bash
{{ ydb-cli }} -e grpc://<node>:2135 admin cluster bridge list
```

Where:

- `<node>` is the host name or IP address of an available cluster node, for example `node-1.example.com`.

Mutating `ydb admin` commands ask for confirmation, while the read-only `bridge list` command does not. Without a TTY, confirmation is silently declined and the command exits with code 1. Scripts must therefore pass the global `-y` option to mutating commands. The examples below omit the endpoint for brevity.

{% note warning "Points of no return" %}

Converting a cluster to bridge mode is irreversible. After conversion and after a persisted bridge feature-level raise caused by pile operations, non-uniform storage, or per-pile storage overrides, nodes that do not support the required level are fenced out. Upgrade every static and dynamic node before enabling a feature. There is no supported way to make a cluster non-bridged again.

{% endnote %}

## Operations overview {#operations}

| Operation | Command or action | Reversible |
| --- | --- | --- |
| Convert to a single-pile bridge | `{{ ydb-cli }} -y admin cluster bridge convert [--skip-storage-pool-conversion]` | No |
| Enable non-uniform storage | Set `bridge_config.enable_non_uniform_storage: true` | No |
| List a pile | Add it to `bridge_config.piles` and its nodes to `hosts`, then replace the configuration | Yes, while it is `INACTIVE` |
| Add a pile | `{{ ydb-cli }} -y admin cluster bridge add-pile --pile P` | Yes, with `remove-pile` |
| Return a pile | `{{ ydb-cli }} -y admin cluster bridge rejoin --pile P` | — |
| Switch the primary pile | `{{ ydb-cli }} -y admin cluster bridge switchover --new-primary P` | — |
| Planned takedown | `{{ ydb-cli }} -y admin cluster bridge takedown --pile P [--new-primary Q]` | `rejoin` |
| Emergency disconnect | `{{ ydb-cli }} -y admin cluster bridge failover --pile P [--pile R] [--new-primary Q]` | `rejoin` |
| Remove a pile | `{{ ydb-cli }} -y admin cluster bridge remove-pile --pile P` | No, the data is deleted |
| Switch a pool's mode | `ydb-dstool pool set-bridge-mode --pool-name N (--bridged \| --ordinary [--retain-pile P])` | Yes |
| View state and progress | `{{ ydb-cli }} admin cluster bridge list [--format json]` | — |

## View current state {#list}

The `list` command shows the current state of each pile, conversion progress, and pending pile operations:

```bash
{{ ydb-cli }} admin cluster bridge list
```

Example output:

```text
Pile pile-a: PRIMARY
Pile pile-b: SYNCHRONIZED
Generation: 4
```

For scripts, use `--format json`. During conversion, the output includes `Conversion from non-bridged cluster is in progress, piles can't be added yet`. During pile addition or removal, it includes `Pending operation on pile P: ADD` or `REMOVE`. If the last attempt failed and is being retried, the output also includes its error. For the output format, see the [command reference](../../../reference/ydb-cli/commands/bridge/list.md).

## Planned `PRIMARY` change (switchover) {#switchover}

If maintenance is planned in the data center or on the equipment where the current `PRIMARY` pile runs, switch the cluster to another pile in advance. Select a pile in the `SYNCHRONIZED` state:

```bash
{{ ydb-cli }} -y admin cluster bridge switchover --new-primary <pile>
```

Where `<pile>` is the name of the new primary pile, for example `pile-b`.

The switchover is performed smoothly: roles go through `PRIMARY/PROMOTED` and end in `SYNCHRONIZED/PRIMARY`.

## Planned pile disconnection (takedown) {#takedown}

Before maintenance that will make a pile unavailable, take it out of the cluster:

```bash
{{ ydb-cli }} -y admin cluster bridge takedown --pile <pile>
```

If disconnecting the current `PRIMARY`, specify a synchronized replacement:

```bash
{{ ydb-cli }} -y admin cluster bridge takedown --pile <current-primary> --new-primary <synchronized-pile>
```

Where:

- `<pile>` is the pile to disconnect, for example `pile-b`;
- `<current-primary>` is the current primary pile, for example `pile-a`;
- `<synchronized-pile>` is a pile in the `SYNCHRONIZED` state, for example `pile-b`.

The pile transitions to `SUSPENDED` and then to `DISCONNECTED`. Further cluster operations run without it. Before starting maintenance, verify with [list](#list) that disconnection has completed and every pile is in the expected state.

After maintenance, return the pile with [rejoin](#rejoin). When ordinary pools exist, observe these additional restrictions:

- Take only one pile out at a time when `mirror-3-dc` pools exist. With one data center out, these pools survive only one more disk failure.
- Ordinary pools located only in the disconnected data center remain unavailable for the whole maintenance period.
- For a short period after a planned takedown, a node that has not received the new configuration can still write to the disconnected pile and return its answers to reads. Those reads may miss writes already made by nodes that know about the disconnect.

## Emergency disconnection of unavailable piles (failover) {#failover}

Synchronous replication means that an unexpected pile failure stops bridged writes by default. To continue without the failed pile, a person or external automation must explicitly perform failover.

First ensure that the failed pile is stopped or isolated and cannot resume serving requests on its own. Never run failover independently on both sides of a network split; see [split-brain recovery](../../../concepts/bridge.md#split-brain). Then run:

```bash
{{ ydb-cli }} -y admin cluster bridge failover --pile <unavailable-pile>
```

If the current `PRIMARY` is unavailable, add `--new-primary` with the name of a pile in the `SYNCHRONIZED` state:

```bash
{{ ydb-cli }} -y admin cluster bridge failover --pile <unavailable-primary> --new-primary <synchronized-pile>
```

Where:

- `<unavailable-pile>` is the failed pile, for example `pile-b`;
- `<unavailable-primary>` is the failed primary pile, for example `pile-a`;
- `<synchronized-pile>` is a healthy synchronized pile, for example `pile-b`.

If several piles fail, list all of them in one command by repeating `--pile`. For example, if `pile-a` and `pile-b` have failed and `pile-c` is still `SYNCHRONIZED`, connect to a node in `pile-c` and run:

```bash
{{ ydb-cli }} -y admin cluster bridge failover --pile pile-a --pile pile-b --new-primary pile-c
```

This single configuration update disconnects both unavailable piles and selects the new `PRIMARY`. Disconnecting them one at a time may fail because the second failed pile is still required by the intermediate configuration quorum. Bridged databases continue running from the remaining pile. Ordinary `mirror-3-dc` databases remain unavailable until one of the failed data centers returns.

For a cluster split into one and two data centers, prefer failing over the single data center. Both bridged and ordinary `mirror-3-dc` databases then keep running on the pair. Keep only the single data center if the pair is really lost; ordinary `mirror-3-dc` pools have no write quorum in that case.

The remaining pile must have the node and storage quorums required to apply the new configuration and serve requests. Failover does not enable automatic failover and does not allow an unsynchronized pile to become `PRIMARY`. Valid transitions depend on the current state pair and are shown in the [state diagram](../../../concepts/bridge.md#pile-states) and [transition table](../../../concepts/bridge.md#transitions-between-states). After the command succeeds, check the result with [list](#list).

{% note warning %}

If mandatory authentication is enabled, login and password authentication cannot be used while the cluster is in an emergency state. Use client certificate authentication instead. For details, see [{#T}](#emergency-auth).

{% endnote %}

## Return a pile to the cluster (rejoin) {#rejoin}

After maintenance or failure recovery, explicitly return each disconnected pile:

```bash
{{ ydb-cli }} -y admin cluster bridge rejoin --pile <pile>
```

Where `<pile>` is the disconnected pile, for example `pile-b`.

The pile transitions to `NOT_SYNCHRONIZED`, and background data synchronization starts. When synchronization completes, it automatically becomes `SYNCHRONIZED`. Bridged parts are synchronized by the bridge mechanism, while ordinary VDisks catch up through normal VDisk recovery. Track progress using [monitoring counters](#monitoring). After the pile becomes `SYNCHRONIZED`, you can [switch the `PRIMARY` role to it](#switchover) if needed.

## Prerequisites for cluster and topology changes {#prerequisites}

Before converting a cluster, adding or removing piles, or changing pool modes, check the following:

1. Every static and dynamic node runs a build that supports the required bridge functionality. Use the [executable update procedure](update-executable.md) if necessary.
2. The cluster uses configuration V2 and self-management is enabled with `self_management_config.enabled: true`. Check the fetched configuration as described below. For configuration V1, complete the [migration to V2](../../configuration-management/migration/migration-to-v2.md) and the [bridge-specific migration steps](#migrate-v2).
3. `default_disk_type` matches the drives that hold static groups: `ROT`, `SSD`, or `NVME`. It defaults to `SSD`.

   - With default allocation, self-management places static groups, including their parts in piles added later, only on drives of this type. If `blob_storage_config.storage_pool_pile_overrides` is configured, its per-pile filters and geometry take precedence; check those drives as well.
   - With the wrong type, `add-pile` fails with `group allocation failed ... PDisks# <empty>`.

4. No BlobDepot decommission is in progress if you intend to use the default all-bridged conversion. Groups being decommissioned keep conversion in progress until they reach `DONE`.
5. Each pile to be added has hardware for:

   - its part of the static groups and State Storage;
   - a full copy of every bridged pool. For `block-4-2`, this means eight failure domains as defined by the pool geometry;
   - its share of ordinary pools. A `mirror-3-dc` pool needs three racks per data center.

Fetch and replace the configuration through an available node:

```bash
{{ ydb-cli }} -e grpc://<node>:2135 admin cluster config fetch > config.yaml
{{ ydb-cli }} -e grpc://<node>:2135 -y admin cluster config replace -f config.yaml
```

Where `<node>` is an available cluster node, for example `node-1.example.com`. Edit `config.yaml` between the commands. The `fetch` command emits the next `metadata.version` for the main configuration.

Nodes read `bridge_config` and their own `location` only at startup. Where these procedures require a rolling restart, restart every static node and then every dynamic node. For the general mechanism, see [maintenance without downtime](../../concepts/maintenance-without-downtime.md).

## Migrate configuration V1 to V2 with self-management {#migrate-v2}

First follow the general [migration to configuration V2](../../configuration-management/migration/migration-to-v2.md). Then apply the bridge-specific checklist:

1. Set `feature_flags.switch_to_config_v2: true`. If Console holds the configuration, put the flag there first.
2. Perform a rolling restart.
3. Put the configuration into V2 storage as is. Then set `self_management_config.enabled: true` and `default_disk_type`, and perform another rolling restart.
4. Remove the sections now managed by distributed configuration:

   - move `domains_config.security_config` to the top level as `security_config`;
   - remove `domains_config` and `blob_storage_config`;
   - rename `static_erasure` to `erasure`.

5. Perform a rolling restart.

Verify the resulting configuration with `admin cluster config fetch` before continuing with conversion.

## Convert a cluster to a single-pile bridge {#convert}

Conversion adds the bridge control plane and converts system storage. Choose one of two modes for existing database pools:

- **Default conversion** makes every pool bridged. Groups become inline piles and keep their IDs and data. No data moves during conversion itself. Each later `add-pile` creates a part for every group, and `rejoin` synchronizes all data at once.
- **Conversion with `--skip-storage-pool-conversion`** immediately converts only system storage. Existing pools stay ordinary, and non-uniform storage is enabled as part of conversion. Later `add-pile` calls synchronize only system data. Convert pools one at a time as described in [changing pool modes](#pool-mode) to control synchronization load. This mode is recommended for large clusters and is required when ordinary pools are planned.

To convert the cluster:

1. Add a bridge configuration with one pile:

   ```yaml
   bridge_config:
     piles:
       - name: A
     enable_non_uniform_storage: true   # REQUIRED with --skip-storage-pool-conversion, FORBIDDEN with default conversion
   ```

   Replace the configuration, then perform a rolling restart of **all** nodes, including dynamic nodes. A connected dynamic node running without the bridge configuration blocks conversion.

2. Run the selected conversion command:

   ```bash
   {{ ydb-cli }} -y admin cluster bridge convert
   {{ ydb-cli }} -y admin cluster bridge convert --skip-storage-pool-conversion
   ```

   These are alternative commands: run only the one for the selected mode. If a node is unfit, the command fails without changing anything and lists its node ID. Restart or upgrade the listed nodes and repeat the command.

3. Wait until [bridge list](#list) no longer prints `Conversion from non-bridged cluster is in progress, piles can't be added yet` and shows `A: PRIMARY`.

Until this message disappears, the pile list cannot change and non-uniform storage cannot be enabled separately. With the default conversion, set `bridge_config.enable_non_uniform_storage: true` only after conversion completes and only if mixed pool modes are required. This setting is irreversible.

A KV and table smoke check is recommended. While State Storage switches to its bridged layout, database resolution can fail briefly. For command details, see [admin cluster bridge convert](../../../reference/ydb-cli/commands/bridge/convert.md).

{% note warning "Multi-data-center clusters" %}

Do not convert a multi-data-center cluster if the goal is one pile per data center. Conversion currently creates one pile spanning every data center. A pile cannot be split afterwards, a node cannot move to another pile without leaving the cluster, and a cluster cannot be converted twice. This is a current limitation of the conversion operation.

{% endnote %}

## Name the converted pile {#name-pile}

The converted pile is implicit: nodes without a pile name belong to it. Before listing a second pile, and before removing the converted pile, assign its name to every node.

1. Add `location.bridge_pile_name: A` to every entry in `hosts` and to the location of every dynamic node. NodeBroker accepts the change from no pile name to the implicit pile's name. See the [hosts configuration](../../../reference/configuration/hosts.md#hosts-bridge).
2. If ordinary `mirror-3-dc` pools are planned, also give A's nodes a `data_center` label that differs from the new data centers. Change it in the same step. Test this on staging first because it changes every node location in A, then run `ydb-dstool group check` afterwards.
3. Replace the configuration and perform a rolling restart.

## List and add a pile {#add-pile}

Adding a pile creates its parts of bridged groups without stopping the cluster. Register the pile and its nodes first, then allocate and synchronize storage:

1. **List the pile** in one configuration replacement:

   - append `{name: B}` to `bridge_config.piles`;
   - add B's `host_configs` and `hosts`, each with `location: {data_center: dc-b, rack: <rack>, bridge_pile_name: B}`, where `<rack>` is the rack identifier;
   - extend `system_tablets`, including `default_node` and every per-tablet `node` list, with B's node IDs. These lists control where system tablets can start. A configuration migrated from V1 binds them to A's nodes, and this is not validated automatically; without B's nodes, nothing would start after failover to B.

2. Start B's static nodes. They remain idle, and `bridge list` shows `B: INACTIVE`.
3. **Add the pile**:

   ```bash
   {{ ydb-cli }} -y admin cluster bridge add-pile --pile B
   ```

   The command starts allocation and preflight as one pending operation. On success, BSC has created B's parts of every bridged group and the pile is `DISCONNECTED`. On a transient BSC error, the pending operation remains and is retried.

   - If there is not enough space, the command fails fast without changing anything.
   - Immediately after registration, it can fail with `group allocation failed` because BSC learns about new hosts in the background. Retry after a few seconds.
   - If addition gets stuck and `bridge list` shows `Pending operation on pile B: ADD` and `Last attempt has failed (being retried): ...`, cancel it with `remove-pile --pile B`.

4. **Return the pile** after the pending `ADD` operation clears:

   ```bash
   {{ ydb-cli }} -y admin cluster bridge rejoin --pile B
   ```

   The pile goes through `NOT_SYNCHRONIZED` to `SYNCHRONIZED`. Track progress with [monitoring counters](#monitoring).

5. Start B's dynamic nodes for every database with `bridge_pile_name: B`. They serve followers now and leaders after switchover or failover.

Only one pile addition or removal can be pending at a time. Several piles can be listed at once, but adding and synchronizing them one after another keeps the load predictable. For command details, see [admin cluster bridge add-pile](../../../reference/ydb-cli/commands/bridge/add-pile.md).

## Change a storage pool mode {#pool-mode}

A bridge cluster with [non-uniform storage](../../../concepts/bridge.md#storage-pools) can contain bridged and ordinary pools. Conversion must be complete, and no pile operation can be pending. Use `ydb-dstool pool set-bridge-mode` to change a pool without changing group IDs or moving data at the database level. Repeat `--pool-name` to change several pools in one request. Database pools are named `<database>:<kind>`, for example `/Root/db1:ssd`.

### Make a pool bridged {#pool-bridged}

To replicate an ordinary pool to every pile:

```bash
ydb-dstool pool set-bridge-mode --pool-name /Root/db1:ssd --bridged
```

Each group must reside inside a single pile. A `mirror-3-dc` group spanning data centers is refused with `resides in several piles` because it cannot be converted directly. Parts in other piles are created immediately, and the command fails without changes if there is not enough space. Parts are synchronized in the background, and groups created afterwards are bridged.

{% note warning %}

Until a group's new parts are synchronized, the group depends only on its original pile. If that pile fails, the group stops taking writes until the pile returns; `failover` does not help. Do not take the original pile down while a pool is synchronizing.

{% endnote %}

This operation also works on a single-pile cluster: groups become inline piles and receive their parts at the next `add-pile`.

### Make a pool ordinary {#pool-ordinary}

To keep one part of every bridged group and delete the others:

```bash
ydb-dstool pool set-bridge-mode --pool-name /Root/db1:ssd --ordinary --retain-pile A
```

`--retain-pile` is optional. By default, each group keeps the part it was made bridged from, its inline pile, or its part in the primary pile. Other parts are deleted in the background, approximately 30 seconds later. The retained part must be `SYNCED`, and its pile must remain in the data quorum. A non-authority retained part also requires its pile to be `SYNCHRONIZED`.

From then on, the pool is unavailable whenever the retained pile is out, and `remove-pile` for that pile is refused. `TDefineStoragePool` cannot change the mode of an existing pool and returns `can't change storage pool BridgeMode`; only `set-bridge-mode` can change it. For command details, see [pool set-bridge-mode](../../../reference/ydb-dstool/pool-set-bridge-mode.md).

## Create ordinary pools and databases {#create-pools}

Non-uniform storage allows new databases to use ordinary groups alongside bridged databases. Enable it as described in [conversion](#convert), and set `bridge_mode` explicitly for every pool. An omitted value is resolved from the cluster's persisted bridge state and is not a safe way to choose the mode during single-pile conversion.

For cross-data-center pools, every involved pile must be connected and not `SUSPENDED` when groups are created or added. BSC does not place new VDisks in `DISCONNECTED`, `INACTIVE`, or `SUSPENDED` piles.

The following configuration fragment defines a bridged pool, an ordinary cross-data-center pool, and an ordinary pool pinned to one data center:

```yaml
storage_pool_types:
  - kind: reliable
    pool_config:
      box_id: 1
      kind: reliable
      erasure_species: block-4-2
      bridge_mode: true
      vdisk_kind: Default
      pdisk_filter: [{property: [{type: SSD}]}]
  - kind: fast
    pool_config:
      box_id: 1
      kind: fast
      erasure_species: mirror-3-dc
      bridge_mode: false
      vdisk_kind: Default
      pdisk_filter: [{property: [{type: SSD}]}]
  - kind: local-a
    pool_config:
      box_id: 1
      kind: local-a
      erasure_species: block-4-2
      bridge_mode: false
      vdisk_kind: Default
      pdisk_filter: [{property: [{type: SSD}, {location: {data_center: dc-a}}]}]
```

The `reliable` kind creates one `block-4-2` group per pile. The `fast` kind creates one ordinary `mirror-3-dc` group across three data centers. The `local-a` kind creates ordinary `block-4-2` groups only in `dc-a`, providing cheaper storage without data-center fault tolerance. For field descriptions, see [storage pool configuration](../../../reference/configuration/domains_config.md#domains-blob).

Create a database with the selected kind, for example:

```bash
ydbd -s grpc://<node>:2135 admin database /Root/fastdb create fast:4
```

Where `<node>` is an available cluster node, for example `node-1.example.com`.

A database behaves as ordinary only if **all** its channels are bound to ordinary pools: data, logs, SchemeShard, coordinators, mediators, and a private Hive if present. Create it using only the ordinary kind. Custom channel profiles, column families or topics bound to other kinds, and serverless databases are not validated by this procedure; serverless databases use their shared database's pools.

An existing pool's erasure cannot change. To move data from a bridged or single-data-center pool to `mirror-3-dc`, move it at the database level using backup and restore or replication. After non-uniform storage is enabled, new groups of an existing ordinary pool may also be placed in other data centers as geometry permits. Add a `location` property to the pool's `pdisk_filter` to preserve their placement.

## Remove a pile {#remove-pile}

Removing a pile permanently deletes its parts of storage groups and their data. Before running the command:

1. Make sure nothing depends only on the pile:

   - No ordinary group may have VDisks in it. Otherwise the command returns `GroupId# G has VDisks in the pile, but it is not a part of bridged group, its data would be lost`. Make those pools bridged, delete them, or move their VDisks off the pile as described in [permanent data-center loss](#dc-loss).
   - No group may rely solely on its part in the pile (`relies on its part in the pile solely`). Wait for `set-bridge-mode` synchronization to finish or retain another pile.

2. If the pile is `PRIMARY`, [switch over](#switchover) first. Then run [takedown](#takedown), or [failover](#failover) if it is already unavailable. The pile must be `DISCONNECTED`.
3. Stop its nodes.
4. Remove the pile:

   ```bash
   {{ ydb-cli }} -y admin cluster bridge remove-pile --pile <pile>
   ```

   Where `<pile>` is the disconnected pile, for example `pile-b`.

   The command starts a pending `REMOVE` operation. BSC deletes the pile's parts and data, then distributed configuration makes the pile `INACTIVE`. Wait for the pending operation to clear before dropping the pile and its hosts from the configuration.

5. Optionally remove the pile and its hosts from the configuration and remove their node IDs from `system_tablets`.

A removed pile can be added again on fresh or the same hosts using the [addition procedure](#add-pile). The pile registry records the new incarnation, so nodes from the old incarnation can be replaced by the current configuration. Wait for the `ADD` operation to clear before `rejoin`. For command details, see [admin cluster bridge remove-pile](../../../reference/ydb-cli/commands/bridge/remove-pile.md).

## Monitoring {#monitoring}

Use both pile state and BSC counters to monitor conversion, synchronization, and mixed storage:

- `ydb admin cluster bridge list` shows pile states, conversion progress, pending `ADD` or `REMOVE` operations, and the last error of an operation being retried. Add `--format json` for scripts.
- BSC counters with `subsystem=bsc` provide synchronization progress:

  - per `pile`, `database`, and `storagePool`: `SyncBytesDone`, `SyncBytesTotal`, `GroupsInStage{stage=...}`, `SyncersRunning`, and `SyncErrors`;
  - per pile: `PileInState{state=...}` and `UnsyncedBSC`.

  These counters are the only place to see the progress of `rejoin` and `set-bridge-mode --bridged`.

- Health check reports problems of bridged groups against their pile. Problems of ordinary groups have no pile.
- Do not rely on BSC forecasts of available groups in mixed clusters because they do not take a pool's bridge mode into account.

## Scenarios {#scenarios}

### One-data-center V1 cluster to a three-data-center mixed bridge {#three-dc}

This scenario creates piles A, B, and C in data centers `dc-a`, `dc-b`, and `dc-c`. Reliable databases use bridged `block-4-2` groups in every pile. Databases that need lower latency or cheaper storage use ordinary `mirror-3-dc` groups across the three data centers.

1. Check the [prerequisites](#prerequisites), then complete the [V1 to V2 migration](#migrate-v2) with self-management and `default_disk_type`.
2. Perform [conversion with `--skip-storage-pool-conversion`](#convert) using `bridge_config: {piles: [{name: A}], enable_non_uniform_storage: true}` and a rolling restart.
3. [Name A's nodes](#name-pile) and give them `data_center: dc-a`.
4. [List and add B](#add-pile), including `system_tablets`; start its static nodes, run `add-pile` and `rejoin`, wait for `SYNCHRONIZED`, and start dynamic nodes.
5. Repeat the same procedure for C.
6. For existing databases that must be reliable, [make their pools bridged](#pool-bridged) one at a time and wait for each synchronization. Keep A running until every pool is synchronized.
7. Create new reliable databases with kind `reliable` and new low-latency or cheaper databases with kind `fast` as described in [pool creation](#create-pools). Create cross-data-center pools while all three piles are connected.
8. For existing databases that should use `mirror-3-dc`, move their data at the database level into a new `fast` database. Existing single-data-center pools left unchanged retain A-only availability and block `remove-pile A`.
9. Verify that all piles are `SYNCHRONIZED` and health check is clean. During a maintenance window, run `switchover --new-primary B` and then switch back.
10. Place `PRIMARY` in the data center closest to clients of low-latency databases because their tablet leaders run there.

With default conversion instead, omit `enable_non_uniform_storage` at step 2. Every existing pool becomes bridged and is synchronized during each `rejoin`. After conversion finishes and before step 7, enable non-uniform storage with a configuration replacement if mixed pools are required.

### Permanent data-center loss {#dc-loss}

If a data center is lost permanently:

1. Perform [failover](#failover).
2. If the lost data center holds ordinary VDisks, list and add the replacement data center first. Mark the lost data center's drives for decommission so SelfHeal moves `mirror-3-dc` rings to the replacement. Validate replacement and SelfHeal behavior on a staging cluster before relying on this sequence.
3. [Remove the lost pile](#remove-pile).
4. List the replacement hosts again, then run `add-pile` and `rejoin` using the [addition procedure](#add-pile).

### Move an ordinary cluster to new hardware or a new data center without downtime {#hardware-move}

Use bridge conversion to move the cluster without database-level data migration:

1. [Convert the cluster](#convert) and [name the original pile](#name-pile).
2. [Add the new pile](#add-pile).
3. Run `switchover --new-primary NEW`, where `NEW` is the new pile's configured name.
4. Run `takedown --pile OLD`, stop OLD's nodes, and [remove OLD](#remove-pile), where `OLD` is the original pile's configured name.

The result is a single-pile bridged cluster. Bridge conversion cannot be reversed, so validate this procedure on a staging cluster first.

## Troubleshooting {#troubleshooting}

| Message or symptom | Cause | Action |
| --- | --- | --- |
| `Self-management must be enabled to convert cluster to bridged one` | Configuration V1 or self-management is disabled | Complete the [V2 migration](#migrate-v2). |
| `BridgeConfig must contain exactly one pile to convert cluster to bridged one` | More than one pile is listed | Leave exactly one pile for [conversion](#convert). |
| `Some connected nodes are running without BridgeConfig or with version not supporting ... NodeIds# ...` | A node was not restarted or runs an old binary | Restart or upgrade the listed nodes. |
| `Some static nodes did not report their BridgeConfig, they must be running and connected NodeIds# ...` | A static node is down or partitioned | Bring it up and retry. |
| `skipping storage pool conversion requires bridge_config.enable_non_uniform_storage` | `--skip-storage-pool-conversion` was used without the flag | Add the flag and perform a rolling restart. |
| `enable non-uniform storage only after BSC conversion completes` | The flag was set for default conversion | Remove the flag and restart, or use `--skip-storage-pool-conversion`. |
| `list of bridge piles can't be changed until BSC finishes conversion of the cluster` | Conversion is still running | Wait for [bridge list](#list) to report completion. |
| `Piles can't be added to the cluster until BSC finishes conversion of the cluster` | Conversion is still running | Wait and check for BlobDepot decommissions. |
| `Operation on pile 'X' is in progress` | Another addition or removal is pending | Wait, or cancel a stuck `ADD` of X with `remove-pile --pile X`. |
| `Pile 'X' has to be INACTIVE to be added ...` or `... DISCONNECTED to be removed ...` | The pile is in the wrong state | Check [bridge list](#list) and the [addition](#add-pile) or [removal](#remove-pile) procedure. |
| `group allocation failed ... PDisks# <empty>` during `add-pile` | BSC has not seen the hosts, `default_disk_type` is wrong, or drives are not `ACTIVE` | Retry, fix `default_disk_type`, and check drive status. |
| `Pile 'A' is the implicit one and NodeId# N has no pile name ...` | The converted pile has not been named | Complete the [naming procedure](#name-pile). |
| `GroupId# G has VDisks in the pile, but it is not a part of bridged group, its data would be lost` | Ordinary VDisks remain in the pile being removed | Perform step 1 of [pile removal](#remove-pile). |
| `GroupId# G relies on its part in the pile solely, its data would be lost` | A pool being made bridged has not synchronized, or an ordinary pool retains this pile | Wait for synchronization or retain another pile. |
| `Static GroupId# G has VDisks in pile 'X', but it is not a part of bridged group` | Static VDisks are outside the bridged layout | Escalate to the storage team. |
| `BridgeMode change requires a bridged cluster with BSC conversion complete` or `BridgeMode override requires a bridged cluster with BSC conversion complete` | Pool mode override was attempted before conversion completed or on an implicit single-pile bridge | Finish conversion and use a named multi-pile bridge. |
| `BridgeMode change requires bridge_config.enable_non_uniform_storage` or `BridgeMode override requires ...` | Non-uniform storage is disabled | Enable it after conversion as described in [conversion](#convert). |
| `BridgeMode can't be changed while the set of piles is being changed` | A pile addition or removal is pending | Wait for it to complete. |
| `GroupId# G resides in several piles, it can't be made bridged` | A cross-data-center ordinary group, such as `mirror-3-dc`, cannot be converted directly | Use database-level data movement instead of direct conversion. |
| `GroupId# G is being decommitted or recommissioned, it can't be made bridged` | BlobDepot decommission is in progress | Wait for `DONE`. |
| `GroupId# G part in pile P is not synchronized` with `--ordinary` | The retained part is not synchronized | Wait or choose another `--retain-pile`. |
| `can't change storage pool BridgeMode` | A pool template was redefined with another `bridge_mode` | Use [set-bridge-mode](#pool-mode) for an existing pool. |
| `non-uniform storage activation can't be disabled` | The flag was removed after activation | Keep the flag in the configuration. |
| Nothing serves requests after failover to a new pile | `system_tablets` is bound to the old pile's nodes | Extend the lists as described in step 1 of [pile addition](#add-pile). |

## Authentication specifics when the cluster is in an emergency state {#emergency-auth}

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
    -y admin cluster bridge failover --pile <unavailable-pile> --new-primary <synchronized-pile>
```

Where:

- `<node.ydb.tech>` is the FQDN of a node in a healthy pile;
- `ca.crt` is the certificate of the cluster's trusted certificate authority;
- `node.crt` and `node.key` are the client certificate and its private key that satisfy the requirements of the `client_certificate_authorization` section;
- `<unavailable-pile>` is the failed pile, for example `pile-a`;
- `<synchronized-pile>` is a healthy synchronized pile, for example `pile-b`.

{% note warning %}

Verify that client certificate authentication works in advance, before an emergency occurs. If client certificate authentication was not configured before the emergency, bringing the cluster out of the emergency state becomes significantly more complicated and requires manual intervention on the cluster nodes.

{% endnote %}
