# bridge_config {#bridge-config}

Секция описывает pile кластера для [режима bridge](../../concepts/bridge.md). Укажите список имён pile, которые используются для привязки хостов и других сущностей. В режиме bridge для каждого хоста также необходимо указать имя соответствующего pile в секции `hosts` (поле `bridge_pile_name`), см. [hosts](./hosts.md#hosts-bridge).

## Синтаксис

```yaml
bridge_config:
  piles:
  - name: <pile_name_1>
  - name: <pile_name_2>
  ...
  - name: <pile_name_n>
```
