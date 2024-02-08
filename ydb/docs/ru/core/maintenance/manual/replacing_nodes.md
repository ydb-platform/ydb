# Замена FQDN узла

Иногда возникает ситуация, когда меняется FQDN узла, при этом сам узел в системе остаётся, но под другим именем. Простая замена имени узла в разделе hosts не сработает, т.к. BS\_CONTROLLER внутри хранит привязку ресурсов к парам FQDN:IcPort, где IcPort — номер порта Interconnect, на котором работает узел.

## Процедура замены

1. Для заменяемого узла определить его NodeId.
2. Подготовить команду DefineBox, которая описывает ресурсы кластера, в которой для ресурсов заменяемого узла будет добавлен элемент `EnforcedNodeId: <NodeId>`.
3. Выполнить эту команду.
4. Заменить FQDN в списке hosts в cluster.yaml.
5. Выполнить rolling restart.
6. Убрать из DefineBox поле EnforcedNodeId и заменить Fqdn на новое название узла.
7. Выполнить DefineBox с новыми значениями.

## Пример

Предположим кластер, состоящий из трёх узлов:

config.yaml:

```
- host: host1.my.sub.net
  node_id: 1
  location: {unit: 12345, data_center: MYDC, rack: r1}
- host: host2.my.sub.net
  node_id: 2
  location: {unit: 23456, data_center: MYDC, rack: r2}
- host: host3.my.sub.net
  node_id: 3
  location: {unit: 34567, data_center: MYDC, rack: r3}
```

DefineBox выглядит так:

```
DefineBox {
    BoxId: 1
    Host { Key { Fqdn: "host1.my.sub.net" IcPort: 19001 } HostConfigId: 1 }
    Host { Key { Fqdn: "host2.my.sub.net" IcPort: 19001 } HostConfigId: 1 }
    Host { Key { Fqdn: "host3.my.sub.net" IcPort: 19001 } HostConfigId: 1 }
}
```

Предположим, что мы хотим переименовать host1.my.sub.net в host4.my.sub.net. Для этого сначала делаем DefineBox следующего вида:

```
DefineBox {
    BoxId: 1
    Host { Key { Fqdn: "host1.my.sub.net" IcPort: 19001 } HostConfigId: 1 EnforcedNodeId: 1 }
    Host { Key { Fqdn: "host2.my.sub.net" IcPort: 19001 } HostConfigId: 1 }
    Host { Key { Fqdn: "host3.my.sub.net" IcPort: 19001 } HostConfigId: 1 }
}
```

Затем изменяем config.yaml:

```
- host: host4.my.sub.net
  node_id: 1
  location: {unit: 12345, data_center: MYDC, rack: r1}
- host: host2.my.sub.net
  node_id: 2
  location: {unit: 23456, data_center: MYDC, rack: r2}
- host: host3.my.sub.net
  node_id: 3
  location: {unit: 34567, data_center: MYDC, rack: r3}
```

Затем делаем rolling restart кластера.

И затем делаем второй раз скорректированный DefineBox:

```
DefineBox {
    BoxId: 1
    Host { Key { Fqdn: "host4.my.sub.net" IcPort: 19001 } HostConfigId: 1 }
    Host { Key { Fqdn: "host2.my.sub.net" IcPort: 19001 } HostConfigId: 1 }
    Host { Key { Fqdn: "host3.my.sub.net" IcPort: 19001 } HostConfigId: 1 }
}
```
