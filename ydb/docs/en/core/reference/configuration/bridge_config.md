# bridge_config

This section describes the cluster piles for bridge mode. Specify the list of pile names used for binding hosts and other entities. In bridge mode, you must also specify the name of the corresponding pile for each host in the `hosts` section (the `bridge_pile_name` field), see [hosts](./hosts.md#hosts-bridge).

## Syntax

```yaml
bridge_config:
  piles:
  - name: <pile_name_1>
  - name: <pile_name_2>
  ...
  - name: <pile_name_n>
```
