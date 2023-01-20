# Перевоз VDisk'ов

{% if  feature_ydb-tool %}

## Увезти один из VDisk'ов с блочного устройства {#moving_vdisk}

Для того чтобы перевезти VDisk'и с блочного устройства, надо зайти на узел по ssh и выполнить следующую команду.

```bash
ydb-dstool.py -e ydb.endpoint vdisk evict --vdisk-ids VDISK_ID1 ... VDISK_IDN
kikimr admin bs config invoke --proto 'Command { ReassignGroupDisk { GroupId: <ID группы хранения> GroupGeneration: <Поколение группы хранения> FailRealmIdx: <FailRealm> FailDomainIdx: <FailDomain> VDiskIdx: <Номер слота> } }'
```
, где ```VDISK_ID1 ... VDISK_IDN``` - это список айдишников вдисков, разделенных пробелами, вида ```[GroupId:GroupGeneration:FailRealmIdx:FailDomainIdx:VDiskIdx], а

* GroupId: <ID группы хранения>
* GroupGeneration: <Поколение группы хранения>
* FailRealmIdx: <FailRealm>
* FailDomainIdx: <FailDomain>
* VDiskIdx: <Номер слота>

Список айдишников вдисков можно получить, например, при помощи команды:

```
ydb-dstool.py -e ydb.endpoint vdisk list --format tsv --columns VDiskId --no-header
```

## Перевезти VDisk'и со сломанного/отсутствующего устройства {#removal_from_a_broken_device}

В случае если SelfHeal выключен или не перевозит VDisk'и, данную операцию придется выполнить вручную.

1. Убедиться в мониторинге, что VDisk действительно в нерабочем состоянии.

2. Получить ```[NodeId:PDiskId]``` нужного диска, например, с помощью команды

    ```bash
    ydb-dstool.py -e ydb.endpoint vdisk list | fgrep VDISK_ID
    ```

3. Выполнить перевоз VDisk'а

    ```bash
    ydb-dstool.py -e ydb.endpoint pdisk set --status BROKEN --pdisk-ids "[NodeId:PDiskId]"
    ```

## Вернуть PDisk после развоза  {#return_a_device_to_work}

1. Убедиться в мониторинге, что PDisk в рабочем состоянии

2. Получить ```[NodeId:PDiskId]``` нужного диска, например, с помощью команды

    ```bash
    ydb-dstool.py -e ydb.endpoint pdisk list
    ```

3. Вернуть PDisk

    ```bash
    ydb-dstool.py -e ydb.endpoint pdisk set --status ACTIVE --pdisk-ids "[NodeId:PDiskId]"
    ```

{% else %}

## Увезти один из VDisk'ов с блочного устройства {#moving_vdisk}

Для того чтобы перевезти VDisk'и с блочного устройства, надо зайти на узел по ssh и выполнить следующую команду.

```bash
kikimr admin bs config invoke --proto 'Command { ReassignGroupDisk { GroupId: <ID группы хранения> GroupGeneration: <Поколение группы хранения> FailRealmIdx: <FailRealm> FailDomainIdx: <FailDomain> VDiskIdx: <Номер слота> } }'
```

Нужную информацию для выполнения команды можно посмотреть во вьювере (ссылка).

## Перевезти VDisk'и со сломанного/отсутствующего устройства {#removal_from_a_broken_device}

В случае если SelfHeal выключен или не перевозит VDisk'и, данную операцию придется выполнить вручную.

1. Убедиться в мониторинге, что VDisk действительно в нерабочем состоянии.

    Записать fqdn узла, ic-port, путь до VDisk'а, pdisk-id

2. Зайти на любой узел кластера

3. Выполнить перевоз VDisk'а

    ```bash
    kikimr admin bs config invoke --proto 'Command { UpdateDriveStatus { HostKey: { Fqdn: "<host>" IcPort: <ic-port>} Path: "<Путь до партлейбла устройства>" PDiskId: <pdisk-id> Status: BROKEN } }'
    ```

## Вернуть PDisk после развоза  {#return_a_device_to_work}

1. Убедиться в мониторинге, что PDisk в рабочем состоянии

    Записать fqdn узла, ic-port, путь до устройства, pdisk-id

2. Зайти на любой узел кластера

3. Вернуть PDisk

    ```bash
    kikimr admin bs config invoke --proto 'Command { UpdateDriveStatus { HostKey: { Fqdn: "<host>" IcPort: <ic-port>} Path: "<Путь до партлейбла устройства>" PDiskId: <pdisk-id> Status: ACTIVE } }'
    ```

{% endif %}
