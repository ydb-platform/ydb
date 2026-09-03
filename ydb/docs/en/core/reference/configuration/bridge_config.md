# bridge_config

This section describes cluster [piles](../../concepts/glossary.md#pile) for [bridge mode](../../concepts/bridge.md). List the pile names used to bind hosts and other entities. In bridge mode, each host must also specify the corresponding pile name in the `hosts` section (`bridge_pile_name` field); see [Bridge mode specifics](hosts.md#hosts-bridge).

## Syntax

```yaml
bridge_config:
  piles:
  - name: <pile_name_1>
  - name: <pile_name_2>
  ...
  - name: <pile_name_n>
```
