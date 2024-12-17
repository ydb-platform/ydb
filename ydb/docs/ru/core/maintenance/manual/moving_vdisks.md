# Перевоз VDisk'ов

Иногда бывает нужно освободить блочное устройство для замены оборудования. Или один из VDisk'ов интенсивно используется и влияет на производительность остальных VDisk'ов, находящихся на том же PDisk'е. В этих случаях необходимо выполнить перевоз VDisk'ов.

## Перевезти один из VDisk'ов с блочного устройства {#moving_vdisk}

Получите список идентификаторов VDisk'ов с помощью утилиты [{{ ydb-short-name }} DSTool](../../reference/ydb-dstool/index.md):

```bash
ydb-dstool -e <bs_endpoint> vdisk list --format tsv --columns VDiskId --no-header
```

Чтобы перевезти VDisk'и с блочного устройства, выполните на узле следующие команды:

```bash
ydb-dstool -e <bs_endpoint> vdisk evict --vdisk-ids VDISK_ID1 ... VDISK_IDN
ydbd admin bs config invoke --proto 'Command { ReassignGroupDisk { GroupId: <ID группы хранения> GroupGeneration: <Поколение группы хранения> FailRealmIdx: <FailRealm> FailDomainIdx: <FailDomain> VDiskIdx: <Номер слота> } }'
```

* `VDISK_ID1 ... VDISK_IDN` — список идентификаторов VDisk'ов, вида `[GroupId:GroupGeneration:FailRealmIdx:FailDomainIdx:VDiskIdx]`. Идентификаторы разделяются пробелами.
* `GroupId` — ID группы хранения.
* `GroupGeneration` — поколение группы хранения.
* `FailRealmIdx` — номер fail realm.
* `FailDomainIdx` — номер fail domain.
* `VDiskIdx` — номер слота.

## Перевезти VDisk'и со сломанного/отсутствующего устройства {#removal_from_a_broken_device}

В случае, если SelfHeal выключен или не перевозит VDisk'и автоматически, перевоз нужно выполнить вручную:

1. Откройте [мониторинг](../../reference/embedded-ui/ydb-monitoring.md) и убедитесь, что VDisk в нерабочем состоянии.
1. Получите `[NodeId:PDiskId]` нужного диска с помощью утилиты [{{ ydb-short-name }} DSTool](../../reference/ydb-dstool/index.md):

    ```bash
    ydb-dstool -e <bs_endpoint> vdisk list | fgrep VDISK_ID
    ```

1. Перевезите VDisk:

    ```bash
    ydb-dstool -e <bs_endpoint> pdisk set --status BROKEN --pdisk-ids "[NodeId:PDiskId]"
    ```

## Вернуть PDisk после развоза {#return_a_device_to_work}

Чтобы вернуть PDisk после развоза:

1. Откройте [мониторинг](../../reference/embedded-ui/ydb-monitoring.md) и убедитесь, что PDisk в рабочем состоянии.
1. Получите `[NodeId:PDiskId]` нужного диска с помощью утилиты [{{ ydb-short-name }} DSTool](../../reference/ydb-dstool/index.md):

    ```bash
    ydb-dstool -e <bs_endpoint> pdisk list
    ```

1. Верните PDisk:

    ```bash
    ydb-dstool -e <bs_endpoint> pdisk set --status ACTIVE --pdisk-ids "[NodeId:PDiskId]"
    ```
