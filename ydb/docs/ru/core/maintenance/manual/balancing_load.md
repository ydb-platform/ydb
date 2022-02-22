# Балансировка нагрузки на диски

В YDB нагрузку на диски можно распределить двумя способами:

## Распределить нагрузку равномерно по группам

На странице [Hive web-viewer](../embedded_monitoring/hive.md#reassign_groups), в нижней части экрана есть кнопка "Reassign Groups".

## Разложить VDisk'и равномерно по устройствам

В случае, если VDisk'и расположены на блочных устройствах не равномерно, можно [перевезти их](moving_vdisks.md#moving_vdisk) по одному с перегруженных устройств.

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
