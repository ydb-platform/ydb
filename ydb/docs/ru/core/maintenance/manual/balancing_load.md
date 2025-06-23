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

    `<bs_endpoint>` - эндпоинт произвольного [узла хранения](../../concepts/glossary.md#storage-node) кластера.
    Команда перевозит не более одного VDisk'а за один запуск.
