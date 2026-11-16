# Internals of the V2 configuration mechanism

The V2 configuration in {{ ydb-short-name }} provides a unified approach to managing cluster settings. While the [{#T}](../devops/configuration-management/configuration-v2/config-overview.md) page within Cluster Administration section describes how to use this mechanism, this article focuses on its technical design. It will be useful for {{ ydb-short-name }} developers and contributors who want to make changes to this mechanism, as well as anyone who wants to gain a deeper understanding of what happens in a {{ ydb-short-name }} cluster when the configuration changes.

The {{ ydb-short-name }} cluster configuration is stored in several locations, and various cluster components coordinate synchronization between them:

- as a set of node configuration files in the file system of each {{ ydb-short-name }} node (required to connect to the cluster when a node starts up).
- in a special metadata storage area on each [PDisk](../concepts/glossary.md#pdisk) (a quorum of PDisks is considered the single source of truth for the configuration).
- in the local database of the [DS Controller](../concepts/glossary.md#ds-controller) tablet (required for the [distributed storage](../concepts/glossary.md#distributed-storage)).
- in the local database of the [Console](../concepts/glossary.md#console) tablet.

Some parameters take effect on a node immediately after the modified configuration is delivered to its location, while others only take effect after the node is restarted.

## Distributed configuration system (Distconf)

[Distconf](../concepts/glossary.md#distributed-configuration) is a V2 configuration management system for a {{ ydb-short-name }} cluster based on [Node Warden](../concepts/glossary.md#node-warden), [storage nodes](../concepts/glossary.md#storage-node), and their [PDisks](../concepts/glossary.md#pdisk). All V2 configuration changes, including initial setup, pass through it.

### The Leader's Role

The central element of the Distconf system is the **leader**: the only node in the cluster that currently has the authority to initiate and coordinate configuration changes. Having a single decision-making point eliminates races and conflicts, ensuring that changes are applied consistently. If the current leader fails, the cluster automatically starts the process of electing a new leader. Later in this section, we will look at how elections work and how the leader manages the cluster.

### Quorum and Source of Truth

A key element of Distconf's reliability is storing the configuration directly on PDisks. Any change is considered successfully applied only when it is written to the metadata area on a **quorum** of disks. This means that even if some nodes or disks fail, the system can recover the current and consistent configuration by reading it from the remaining ones. It is the quorum storage on PDisks, not the state of any single node, that is the single source of truth about the cluster configuration. This mechanism is implemented in [`distconf_persistent_storage.cpp`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/nodewarden/distconf_persistent_storage.cpp).

It is worth noting that writing to the PDisk metadata area for Distconf is a special low-level operation that occurs directly through `NodeWarden` and **does not use** the standard data path through `DSProxy`.

The quorum mechanism itself is hierarchical and implemented in [`distconf_quorum.h`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/nodewarden/distconf_quorum.h). The basic principle is "majority of majorities". To make a decision (for example, to apply a new configuration), the following is required:

1. A majority of responding disks within each data center. A data center that meets this condition is considered "operational".
2. A majority of "operational" data centers in the cluster.

For configurations affecting static storage groups, the requirement is even stricter: each static group must confirm its own internal quorum of VDisks in accordance with its fault tolerance scheme.

### Binding Process and Leader Election

Leader election in Distconf is carried out through the `Binding` process, described in [`distconf_binding.cpp`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/nodewarden/distconf_binding.cpp).

1. **Discovery**: Storage nodes get a full list of static cluster nodes through the standard `TEvInterconnect::TEvNodesInfo` mechanism.
2. **Building the binding tree**: Each node initiates a `bind` to a random node from the common list to which it is not yet subordinate. This process forms an acyclic graph, which in intermediate stages is a forest (a set of disconnected trees), and eventually a single tree spanning all nodes.
3. **Cycle prevention and topology exchange**: The cycle prevention mechanism is based on the constant exchange of information about the current topology. When node `A` tries to connect to node `B`, it sends its entire known subtree to `B`. Node `B` rejects the request if `A` is already its descendant. In case of a simultaneous attempt at a mutual connection, the conflict is resolved in favor of the node with the greater `NodeId` value.
4. **Leader determination**: In the process of merging trees, there inevitably remains one node that cannot connect to anyone other node because all other nodes are already in its subtree. This node becomes the root of the final tree and is declared the **leader**.

If the current leader fails, the `Binding` process is restarted, and the cluster elects a new leader.

### Scatter/Gather — Command Propagation Mechanism

To manage the cluster, the leader uses the `Scatter/Gather` mechanism ([`distconf_scatter_gather.cpp`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/nodewarden/distconf_scatter_gather.cpp)), which operates on top of the tree built during the `Binding` process.

- **Scatter**
  When the leader needs to send a command to all nodes (for example, to propose a new configuration), it sends it to its direct children in the tree. Each node, upon receiving the command, retransmits it to its children. This is how the command is efficiently propagated throughout the tree to the leaves.
- **Gather**
  After executing the command, the nodes must report the result. The responses are collected in reverse order: the leaves send the results to their parents, who in turn aggregate them and send them up. As a result, the leader receives a generalized result from the entire cluster.

This mechanism is used for distributed operations, including a two-phase commit when changing the configuration. Scatter and gather tasks (`TScatterTasks`) are tracked by the leader to monitor the execution of operations.

### Finite-State Machine (FSM) and Change Lifecycle

The entire configuration change process on the leader is managed by a finite-state machine (FSM), implemented in [`distconf_fsm.cpp`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/nodewarden/distconf_fsm.cpp). The FSM ensures that only one configuration change operation is performed at a time, preventing races and conflicts.

When a change request is received, the FSM transitions to the `IN_PROGRESS` state, blocking new requests.

The leader uses `Scatter/Gather` to perform a two-phase commit: first, it sends a Propose, and after receiving a quorum of confirmations, a Commit command.

After the successful completion or cancellation of the operation, the FSM returns to the `RELAX` state, allowing the processing of subsequent requests.

### Configuration Management via InvokeOnRoot

[`TEvNodeConfigInvokeOnRoot`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/protos/blobstorage_distributed_config.proto#L177) requests are a unified mechanism for any cluster configuration changes. These commands can be initiated by both a **system administrator** (for example, via the CLI) and **other YDB components** in automatic mode (for example, by the `BlobStorageController` tablet during the `SelfHeal` process).

Regardless of the source, any such request is processed according to the same scenario:

1. The request is delivered to the Distconf leader node (if it was not sent to it, it is redirected automatically).
2. The leader starts the FSM and performs the Scatter/Gather process as described above.

This mechanism ensures that any configuration change, regardless of its nature, goes through a strict validation and quorum confirmation procedure.

The main commands supported by this mechanism:

- `UpdateConfig`: Make partial changes to the current configuration. The changes are transmitted as a `TStorageConfig` protobuf message.
- `QueryConfig`: Request the current and proposed configuration. The response contains `TStorageConfig` protobuf messages.
- `ReplaceStorageConfig`: Replace the current configuration with a new one, passed as YAML.
- `FetchStorageConfig`: Return the loaded YAML cluster configuration.
- `ReassignGroupDisk`: Replace a disk in a static storage group.
- `StaticVDiskSlain`: Handle a VDisk failure event in a static group.
- `DropDonor`: Remove donor disks after data migration is complete.
- `BootstrapCluster`: Initiate the initial creation of the cluster.

### StateStorage selection validation and emergency repair

For every supplied StateStorage ring group, `NToSelect` must be between one and the number of configured rings. A ring containing several nodes still counts as one ring. In the legacy `Node` representation, each node represents a separate one-node ring. Configuration admission rejects invalid values without changing them.

Self-management can generate omitted StateStorage sections. It does not ignore a supplied legacy `domains_config.state_storage` section: that section is used during startup and migration into the managed configuration, so its selection bounds are validated too.

If an already committed configuration has an invalid selection, Distconf keeps its metadata available for diagnosis but does not install or propagate that configuration. Older or conflicting configurations cannot replace the rejected committed version. The following emergency procedure submits a corrected **full `TStorageConfig` protobuf** through `UpdateConfig`; it is not the ordinary YAML `ReplaceStorageConfig` workflow.

The root must have a connected node quorum for its Base configuration. Configuration collection and the new proposition must also satisfy the required configuration and disk quorums, including static storage-group quorums where applicable. The repair does not bypass conflicting history, missing quorums, or uncommitted successors that might already have been chosen. A disconnected set of disks containing historical metadata is not sufficient by itself.

1. Back up the latest committed metadata and its main YAML using the read-only metadata command. Add the deployment's `--main-key NUM` option if the disk requires it.

   ```bash
   ydbd admin bs disk metadata read /dev/DEVICE --json --committed-yaml current.yaml > metadata.json
   ```

2. Extract the full configuration into an editable JSON file. If a dedicated storage YAML document is present, also extract it. The JSON emitted by this command preserves protobuf field names and encodes compressed byte fields as base64. These commands require Python 3 and `zstd`.

   ```bash
   python3 - <<'PY'
   import base64
   import json
   from pathlib import Path
   import subprocess

   config = json.loads(Path("metadata.json").read_text())["CommittedStorageConfig"]
   Path("replacement-config.json").write_text(json.dumps(config, indent=2))
   if "CompressedStorageYaml" in config:
       data = base64.b64decode(config["CompressedStorageYaml"])
       decoded = subprocess.run(["zstd", "-q", "-d", "-c"], input=data,
                                stdout=subprocess.PIPE, check=True).stdout
       Path("current-storage.yaml").write_bytes(decoded)
   PY
   ```

3. Correct the affected `NToSelect` values in the top-level `StateStorageConfig`, `StateStorageBoardConfig`, and `SchemeBoardConfig` sections of `replacement-config.json`. Preserve the remaining topology, storage settings, and accepted history.

   Correct the corresponding supplied sections in `current.yaml`, save it as `corrected-main.yaml`, and advance its `metadata.version`. If dedicated storage YAML is present, correct it as `corrected-storage.yaml`, advance that document's `metadata.version`, and set `ExpectedStorageYamlVersion` in `replacement-config.json` to the new storage YAML version plus one. Fixing only the visible protobuf values leaves the embedded YAML inconsistent. Do not edit PDisk metadata directly.

4. Save the following as `pack-repair.py`. `ConfigComposite` contains a zstd-compressed frame with the main YAML followed by a fetched-YAML component. This Linux little-endian recipe matches the internal framing for documents smaller than 4 GiB; the fetched component is empty, as in the existing metadata CLI writer.

   ```python
   import base64
   import json
   from pathlib import Path
   import struct
   import subprocess
   import sys

   def compress(data):
       encoded = subprocess.run(["zstd", "-q", "-c"], input=data,
                                stdout=subprocess.PIPE, check=True).stdout
       return base64.b64encode(encoded).decode("ascii")

   if len(sys.argv) not in (3, 4):
       raise SystemExit("Usage: pack-repair.py CONFIG MAIN_YAML [STORAGE_YAML]")
   config = json.loads(Path(sys.argv[1]).read_text())
   main = Path(sys.argv[2]).read_bytes()
   if len(main) >= 0xffffffff:
       raise SystemExit("Main YAML is too large")
   if "CompressedStorageYaml" in config and len(sys.argv) != 4:
       raise SystemExit("Provide corrected dedicated storage YAML")
   frame = struct.pack("<I", len(main)) + main + struct.pack("<I", 0)
   config["ConfigComposite"] = compress(frame)
   if len(sys.argv) == 4:
       config["CompressedStorageYaml"] = compress(Path(sys.argv[3]).read_bytes())
   config.pop("Fingerprint", None)
   Path("repair-request.json").write_text(json.dumps({"UpdateConfig": {"Config": config}}))
   ```

   Run it with the corrected documents, omitting the final argument only when the configuration has no dedicated storage YAML:

   ```bash
   python3 pack-repair.py replacement-config.json corrected-main.yaml corrected-storage.yaml
   ```

5. Submit the request to a node's protected monitoring endpoint, using the deployment's normal authentication options:

   ```bash
   curl -H 'Content-Type: application/json' --data-binary @repair-request.json \
     'http://NODE:MONITOR_PORT/actors/nodewarden?page=distconf'
   ```

   The server validates the final protobuf selections and the resolved supplied selections in embedded YAML. It chooses a generation above the observed history, recomputes the fingerprint, preserves the accepted predecessor, and commits through the normal joint write quorum. The caller does not mark the replacement committed or rewrite metadata on individual disks.

6. Inspect the returned RPC status and error; HTTP success alone does not establish that the repair succeeded. Require status `OK`, then submit `{"QueryConfig":{"RequireFresh":true}}` to the same endpoint. Verify an accepted generation greater than the rejected one, convergence of the nodes, and persisted metadata through the deployment's incident procedure.

### Integration and Additional Mechanisms

In addition to the main processes, Distconf works closely with other system components:

- **Distributed Storage Controller**

  The DS-controller receives configuration changes from Distconf and uses them for the operation of the distributed storage.
- **Database nodes**

  Database nodes subscribe to [`TEvNodeWardenDynamicConfigPush`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/nodewarden/node_warden_events.h#L75) events to receive real-time configuration updates.

- **SelfHeal**

  When using Distconf for a [static group](../concepts/glossary.md#static-group), [SelfHeal](../devops/concepts/selfheal-storage.md) works similarly to [dynamic groups](../concepts/glossary.md#dynamic-group).

- **Local YAML files on nodes**

  These are stored in the directory specified by the `ydbd --config-dir` server startup argument and are updated upon receiving information about each configuration change. These files are needed when a node starts to discover PDisks and other cluster nodes, as well as to establish initial network connections. The data in these files may be outdated, especially if the node has been shut down for a long time.

### Basic Distconf workflow

1. When a node starts, Distconf tries to read the configuration from local PDisks.
2. It connects to a random storage node to check if the configuration is up-to-date.
3. If it fails to connect, but has a quorum of connected nodes, the node becomes the leader.
4. The leader tries to initiate the initial cluster setup, if allowed.
5. The leader sends the current configuration to all nodes via Scatter/Gather.
6. Nodes save the received configuration in the local PDisk metadata area and in the `--config-dir` directories.

## Final configuration distribution

| Storage location | Contains |
|---|---|
| `TPDiskMetadataRecord` on a quorum of PDisks | The true current configuration |
| Local `--config-dir` directory | Initial YAML for startup (may be outdated) |
| Console | Up-to-date copy (with minimal delay) |
| DS-controller | Subset of the configuration required for distributed storage |
