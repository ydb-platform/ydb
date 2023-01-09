# Балансировка нагрузки на диски

В {{ ydb-short-name }} нагрузку на диски можно распределить двумя способами:

## Распределить нагрузку равномерно по группам

На странице [Hive web-viewer](../embedded_monitoring/hive.md#reassign_groups), в нижней части экрана есть кнопка "Reassign Groups".

## Разложить VDisk'и равномерно по устройствам

Когда VDisk'и расположены на блочных устройствах неравномерно, можно улучшить равноменость двумя способами: ручным и полуавтоматическим.

В ручном случае можно [перевезти VDisk'и](moving_vdisks.md#moving_vdisk) по одному с перегруженных устройств. В полуавтоматическом случае
можно перевезти VDisk'и по одному с перегруженных устройств с помощью команды:

```
ydb-dstool.py -e ydb.endpoint cluster balance
```

Команда перевозит не более одного VDisk'а за запуск.

## Изменение количествa слотов для VDisk'ов на PDisk'ах

Для добавления групп хранения требуется переопределить конфиг хоста, увеличив для него количество слотов на PDisk'ах.

Перед этим требуется получить изменяемые конфиг, это можно сделать следующей командой:

```proto
Command {
  TReadHostConfig{
    HostConfigId: <host-config-id>
  }
}
```
    
```
kikimr -s <endpoint> admin bs config invoke --proto-file ReadHostConfig.txt
```

Требуется вставить полученный конфиг в протобуф ниже и поменять в нем поле **PDiskConfig/ExpectedSlotCount**.

```proto
Command {
  TDefineHostConfig {
    <хост конфиг>
  }
}
```
    
```
kikimr -s <endpoint> admin bs config invoke --proto-file DefineHostConfig.txt
```
