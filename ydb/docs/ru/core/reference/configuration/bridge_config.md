## bridge_config — конфигурация режима bridge {#bridge-config}

Секция описывает pile кластера для [режима bridge](../../concepts/bridge.md). Укажите список имен pile, которые используются для привязки хостов и других сущностей.

### Синтаксис

```yaml
bridge_config:
  piles:
  - name: <pile_name_1>
  - name: <pile_name_2>
  ...
  - name: <pile_name_n>
```
