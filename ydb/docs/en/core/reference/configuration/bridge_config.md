# bridge_config

This section describes cluster [piles](../../concepts/glossary.md#pile) and storage modes for [bridge mode](../../concepts/bridge.md). Each host must specify its pile in the `hosts` section with the `location.bridge_pile_name` field; see [Bridge mode specifics](hosts.md#hosts-bridge). Nodes read `bridge_config` and their own `location` only at startup, so changing these settings requires a rolling restart of static and dynamic nodes.

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
| `enable_non_uniform_storage` | `false` | Allows [bridged and ordinary databases](../../concepts/bridge.md#storage-pools) to coexist in one cluster. This setting cannot be disabled after activation. |

During default conversion of a non-bridged cluster, leave `enable_non_uniform_storage` unset until conversion completes. Conversion with `--skip-storage-pool-conversion` requires it to be `true` before conversion starts. For the procedure, see [Converting a cluster](../../devops/deployment-options/manual/bridge-management.md#convert).
