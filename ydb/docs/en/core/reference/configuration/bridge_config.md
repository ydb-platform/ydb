# bridge_config

This section describes cluster [piles](../../concepts/glossary.md#pile) and storage modes for [bridge mode](../../concepts/bridge.md). Each host must specify its pile in the `hosts` section with the `location.bridge_pile_name` field. See [Bridge mode specifics](hosts.md#hosts-bridge). A node reads its own location at startup. Changes to an existing node’s pile assignment require a restart. S3 settings of backup piles can be updated online.

## Syntax

```yaml
bridge_config:
  piles:
  - name: <pile_name_1>
  - name: <pile_name_2>
  ...
  - name: <pile_name_n>
  enable_non_uniform_storage: <true_or_false>
```

## Parameters {#parameters}

| Parameter | Default value | Description |
| --- | --- | --- |
| `piles` | `[]` | Lists piles configured in the cluster. A listed pile without allocated storage resources is `INACTIVE`. |
| `enable_non_uniform_storage` | `false` | Allows [bridged and ordinary databases](../../concepts/bridge.md#storage-pools) to coexist in one cluster. This setting cannot be disabled in an active bridge cluster; when [returning to ordinary mode](../../devops/deployment-options/manual/bridge-management.md#rollback), it is removed together with `bridge_config`. |

During default conversion of a non-bridged cluster, leave `enable_non_uniform_storage` unset until conversion completes. Conversion with `--skip-storage-pool-conversion` requires it to be `true` before conversion starts. For the procedure, see [Converting a cluster](../../devops/deployment-options/manual/bridge-management.md#convert).

## Backup piles {#backup-piles}

Omitting `type` selects `REGULAR`. A pile of type `BACKUP` uses a BlobDepot virtual group for each dynamic bridged group. Static groups and StateStorage replicas use local disks and nodes. During initial deployment the first regular pile becomes primary and backup piles start `INACTIVE`.

```yaml
bridge_config:
  piles:
    - name: pile-a
    - name: archive
      type: BACKUP
      backup:
        channel_profiles:
          - storage_pool_name: archive-local
            channel_kind: System
            count: 2
          - storage_pool_name: archive-local
            channel_kind: Data
            count: 2
        s3:
          settings:
            endpoint: s3.example.com
            scheme: HTTPS
            bucket: ydb-archive
            object_key_pattern: production/cluster-a
            access_key: <access-key>
            secret_key: <secret-key>
          async_mode: {}
```

Each channel profile names an operator-defined physical storage pool dedicated to this pile. Bind the first two channels as `System` and include at least one `Data` channel. The `count` field defaults to one. The pile type and channel bindings can be changed while the pile is `INACTIVE`.

The `s3` section uses BlobDepot S3 settings and requires `async_mode`. BlobDepot acknowledges a write after recording it on pile-local storage, then uploads eligible data to S3 in the background. Each BlobDepot appends its assigned name to `object_key_pattern` to form its key prefix in the bucket. The S3 bucket holds payloads. The local tablet metadata and static groups are also needed to serve the pile.

The cluster distributes the configuration, including S3 access keys, to its nodes. Restrict access to configuration files, node bootstrap credentials, and the interconnect network. Nodes exchange the storage configuration during the initial interconnect handshake, before interconnect TLS starts.

For pool creation, credential rotation, and promotion, see [Manage a backup pile](../../devops/deployment-options/manual/bridge-management.md#backup-pile).
