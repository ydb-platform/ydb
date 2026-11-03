# bridge_config {#bridge-config}

The `bridge_config` section configures [piles](../../concepts/glossary.md#pile) and mixed storage for [bridge mode](../../concepts/bridge.md). Use it to declare pile names and optionally allow bridged and ordinary storage pools to coexist.

Each host must specify its pile in the `hosts` section with the `location.bridge_pile_name` field; see [Bridge mode specifics](hosts.md#hosts-bridge). Nodes read `bridge_config` and their own `location` only at startup, so changing these settings requires a rolling restart of static and dynamic nodes.

## Syntax {#syntax}

```yaml
bridge_config:
  piles:
  - name: <pile_name_1>
  - name: <pile_name_2>
  ...
  - name: <pile_name_n>
  enable_non_uniform_storage: <true_or_false>
```

Where:

- `<pile_name_1>`, `<pile_name_2>`, and `<pile_name_n>` are unique pile names, for example `pile-a`, `pile-b`, and `pile-c`;
- `<true_or_false>` is `true` to enable non-uniform storage or `false` to leave it disabled.

## Parameters {#parameters}

| Parameter | Default value | Description |
| --- | --- | --- |
| `piles` | `[]` | Lists piles configured in the cluster. A listed pile without allocated resources is `INACTIVE`. |
| `enable_non_uniform_storage` | `false` | Enables [non-uniform storage](../../concepts/bridge.md#storage-pools), allowing bridged and ordinary pools in one cluster. This setting is irreversible after activation. |

During default conversion of a non-bridged cluster, leave `enable_non_uniform_storage` unset until conversion completes. Conversion with `--skip-storage-pool-conversion` requires it to be `true` before conversion starts. For the procedure, see [Converting a cluster](../../devops/deployment-options/manual/bridge-management.md#convert).
