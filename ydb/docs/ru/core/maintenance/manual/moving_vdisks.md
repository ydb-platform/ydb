# Перевоз VDisk'ов

Иногда бывает нужно освободить блочное устройство для замены оборудования. Или один из VDisk'ов интенсивно используется и влияет на производительность остальных VDisk'ов, находящихся на том же PDisk'е. В этих случаях необходимо выполнить перевоз VDisk'ов.

## Перевезти один VDisk с блочного устройства {#moving_vdisk}

Получите идентификатор VDisk с помощью утилиты [{{ ydb-short-name }} DSTool](../../reference/ydb-dstool/index.md):

```bash
ydb-dstool -e <bs_endpoint> vdisk list --format tsv --columns VDiskId --no-header
```

Переместите выбранный VDisk:

```bash
ydb-dstool -e <bs_endpoint> vdisk evict --vdisk-ids VDISK_ID
```

Blob Storage Controller выбирает подходящий целевой PDisk в соответствии с правилами размещения кластера. `VDISK_ID` — идентификатор VDisk в формате `[GroupId:GroupGeneration:FailRealmIdx:FailDomainIdx:VDiskIdx]`.

## Перевезти все VDisk'и для планового обслуживания {#moving_pdisk}

### Автоматический перенос при плановом обслуживании

Для штатного обслуживания оборудования задайте исходному PDisk maintenance-статус `LONG_TERM_MAINTENANCE_PLANNED`:

```bash
ydb-dstool -e <bs_endpoint> pdisk set \
  --maintenance-status LONG_TERM_MAINTENANCE_PLANNED \
  --pdisk-ids "[NodeId:PDiskId]"
```

Этот статус запрещает размещение новых VDisk'ов на PDisk и указывает SelfHeal асинхронно переместить существующие VDisk'и. Blob Storage Controller выбирает подходящие целевые PDisk'и в соответствии с правилами размещения кластера.

Если после обслуживания PDisk остаётся в кластере и снова может принимать новые VDisk'и, снимите запрос на обслуживание:

```bash
ydb-dstool -e <bs_endpoint> pdisk set \
  --maintenance-status NO_REQUEST \
  --pdisk-ids "[NodeId:PDiskId]"
```

### Управляемый ручной перенос

Если требуется контролировать, какие VDisk'и перемещаются, запретите новые размещения на исходном PDisk и переместите выбранные VDisk'и вручную:

1. Задайте исходному PDisk maintenance-статус `NO_NEW_VDISKS`:

    ```bash
    ydb-dstool -e <bs_endpoint> pdisk set \
      --maintenance-status NO_NEW_VDISKS \
      --pdisk-ids "[NodeId:PDiskId]"
    ```

1. Получите идентификаторы VDisk'ов, расположенных на исходном PDisk:

    ```bash
    ydb-dstool -e <bs_endpoint> vdisk list \
      --format tsv --columns VDiskId NodeId:PDiskId --no-header \
      | fgrep '[NodeId:PDiskId]'
    ```

1. Переместите выбранные VDisk'и:

    ```bash
    ydb-dstool -e <bs_endpoint> vdisk evict --vdisk-ids VDISK_ID1 ... VDISK_IDN
    ```

* `VDISK_ID1 ... VDISK_IDN` — идентификаторы VDisk'ов в формате `[GroupId:GroupGeneration:FailRealmIdx:FailDomainIdx:VDiskIdx]`, разделённые пробелами.
* `NodeId:PDiskId` — идентификатор исходного PDisk.

Если после операции PDisk снова может принимать новые VDisk'и, верните ему maintenance-статус `NO_REQUEST`.

## Воспроизвести нагрузку PDisk на другом устройстве {#testing_device}

Используйте [`pdisk populate`](../../reference/ydb-dstool/pdisk-populate.md) только для контролируемого тестирования устройств, когда требуется переместить на новое устройство точно такой же набор VDisk'ов и сравнить его производительность со старым устройством под той же нагрузкой.

Сначала сохраните активные VDisk'и старого PDisk в снапшот:

```bash
ydb-dstool -e <bs_endpoint> pdisk populate \
  --snapshot-from-pdisk '[SourceNodeId:SourcePDiskId]' \
  --snapshot-file /tmp/source-pdisk.json
```

Проверьте снапшот, затем разместите те же VDisk'и на новом PDisk:

```bash
ydb-dstool -e <bs_endpoint> pdisk populate \
  --destination-pdisk '[DestinationNodeId:DestinationPDiskId]' \
  --snapshot-file /tmp/source-pdisk.json
```

В отличие от `vdisk evict`, эта команда размещает все выбранные VDisk'и на явно указанном целевом PDisk.

## Перевезти VDisk'и со сломанного/отсутствующего устройства {#removal_from_a_broken_device}

В случае, если SelfHeal выключен или не перевозит VDisk'и автоматически, перевоз нужно выполнить вручную:

1. Откройте [мониторинг](../../reference/ydb-ui/ydb-monitoring.md) и убедитесь, что VDisk в нерабочем состоянии.
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

1. Откройте [мониторинг](../../reference/ydb-ui/ydb-monitoring.md) и убедитесь, что PDisk в рабочем состоянии.
1. Получите `[NodeId:PDiskId]` нужного диска с помощью утилиты [{{ ydb-short-name }} DSTool](../../reference/ydb-dstool/index.md):

    ```bash
    ydb-dstool -e <bs_endpoint> pdisk list
    ```

1. Верните PDisk:

    ```bash
    ydb-dstool -e <bs_endpoint> pdisk set --status ACTIVE --pdisk-ids "[NodeId:PDiskId]"
    ```
