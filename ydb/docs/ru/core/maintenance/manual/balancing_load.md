# Балансировка нагрузки на диски

В {{ ydb-short-name }} балансировать нагрузку на диски можно двумя способами:

* [распределить нагрузку равномерно по группам](#reassign-groups);
* [распределить VDisk'и равномерно по устройствам](#cluster-balance).

## Распределить нагрузку равномерно по группам {#reassign-groups}

На странице [Hive web-viewer](../../reference/embedded-ui/hive.md#reassign_groups), в нижней части экрана есть кнопка "Reassign Groups".

## Распределить VDisk'и равномерно по устройствам {#cluster-balance}

В результате некоторых операций, например [декомиссии](../../devops/deployment-options/manual/decommissioning.md), VDisk'и могут быть распределены на блочных устройствах неравномерно. Улучшить равномерность распределения можно одним из способов:

* [Перевезти VDisk'и](moving_vdisks.md#moving_vdisk) по одному с перегруженных устройств.
* Воспользоваться утилитой [{{ ydb-short-name }} DSTool](../../reference/ydb-dstool/index.md). Следующая команда перевезет VDisk с перегруженного устройства на менее нагруженное:

    ```bash
    ydb-dstool -e <bs_endpoint> cluster balance
    ```

    Команда перевозит не более одного VDisk'а за один запуск.

## Изменение количествa слотов для VDisk'ов на PDisk'ах

Для добавления групп хранения требуется переопределить конфиг хоста, увеличив для него количество слотов на PDisk'ах. Это можно осуществить, проделав следующие шаги:

1. Получить текущую конфигурацию кластера:

```bash
ydb -e <endpoint> admin cluster config fetch > config.yaml
```

2. Добавить (или изменить) поле `expected_slot_count` для нужного устройства `drive` в секции `host_configs`

3. Загрузить обновленный конфигурационный файл на кластер:

```bash
ydb -e <endpoint> admin cluster config replace -f config.yaml
```
