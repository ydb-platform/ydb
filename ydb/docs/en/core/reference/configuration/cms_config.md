# cms_config

[Cluster Management System (CMS)](../../concepts/glossary.md#cms) is a {{ ydb-short-name }} component that enables [safe cluster maintenance](../../devops/concepts/maintenance-without-downtime.md), for example, updating its version or replacing failed disks without loss of availability. CMS behavior is configured in the `cms_config` section of the {{ ydb-short-name }} configuration.

## Syntax

```yaml
cms_config:
  tenant_limits:
    disabled_nodes_limit: 2
    disabled_nodes_ratio_limit: 5
  cluster_limits:
    disabled_nodes_limit: 3
    disabled_nodes_ratio_limit: 5
  disable_maintenance: true
```

## Parameters

| Parameter | Default value | Description |
| --- | --- | --- |
| `tenant_limits.disabled_nodes_limit` | — | Maximum number of [database nodes](../../concepts/glossary.md#database-node) that can be simultaneously unavailable or blocked. |
| `tenant_limits.disabled_nodes_ratio_limit` | `13` | Maximum percentage of [database nodes](../../concepts/glossary.md#database-node) that can be simultaneously unavailable or blocked. |
| `cluster_limits.disabled_nodes_limit` | — | Maximum number of [cluster](../../concepts/glossary.md#cluster) nodes that can be simultaneously unavailable or blocked. |
| `cluster_limits.disabled_nodes_ratio_limit` | `13` | Maximum percentage of [cluster](../../concepts/glossary.md#cluster) nodes that can be simultaneously unavailable. |
| `disable_maintenance` | `false` | Flag that suspends new cluster maintenance operations. |
| `sentinel_config.enable` | `true` | Enables Sentinel, the CMS component that monitors disks and initiates [storage SelfHeal](../../devops/concepts/selfheal-storage.md). |
| `sentinel_config.dry_run` | `false` | If `true`, Sentinel does not send disk status changes to the Blob Storage Controller. |
| `sentinel_config.evict_vdisks_status` | `FAULTY` | Selects the [VDisk eviction mode](#evict-vdisks-status) before host maintenance: `FAULTY`, `MAINTENANCE`, or `DISABLED`. The value `UNKNOWN` is equivalent to `FAULTY`. |

## VDisk Eviction Mode Before Maintenance {#evict-vdisks-status}

The `sentinel_config.evict_vdisks_status` parameter determines how Sentinel initiates [VDisk](../../concepts/glossary.md#vdisk) relocation for a CMS request with `EvictVDisks` enabled. In the `ydbd cms request` command, this option is set by the `--evict-vdisks` flag. Changing the parameter alone does not create a maintenance request.

| Value | Behavior |
| --- | --- |
| `FAULTY` | Sentinel marks the [PDisks](../../concepts/glossary.md#pdisk) of the host being maintained as `FAULTY` so that SelfHeal relocates VDisks to other disks. These PDisks count as faulty in storage group fault tolerance checks. |
| `MAINTENANCE` | Sentinel sets a separate maintenance status, `LONG_TERM_MAINTENANCE_PLANNED`. A healthy PDisk remains `ACTIVE` and does not count as faulty solely because of planned maintenance. SelfHeal relocates VDisks subject to the [planned relocation conditions](../../devops/concepts/selfheal-storage.md#planned-maintenance). |
| `DISABLED` | CMS rejects requests with `EvictVDisks` enabled. Regular disk fault monitoring continues to operate. |

If the parameter is omitted or set to `UNKNOWN`, the effective mode is `FAULTY`.

To enable planned maintenance mode, add the following settings to the `config` section of the [cluster configuration](../../devops/configuration-management/index.md):

```yaml
cms_config:
  sentinel_config:
    enable: true
    dry_run: false
    evict_vdisks_status: MAINTENANCE
```

This example is a configuration fragment. [Storage SelfHeal must also be enabled](../../devops/concepts/selfheal-storage.md#on-off) for VDisks to be relocated.
