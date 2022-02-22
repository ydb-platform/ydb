# Перевоз VDisk'ов

## Увезти один VDisk'ов с блочного устройтства {#moving_vdisk}

Для того чтобы перевезти VDisk'и с блочного устройства, надо зайти на ноду по ssh и выполнить следующую команду.

```bash
kikimr admin bs config invoke --proto 'Command { ReassignGroupDisk { GroupId: <ID группы хранения> GroupGeneration: <Поколение группы хранения> FailRealmIdx: <FailRealm> FailDomainIdx: <FailDomain> VDiskIdx: <Номер слота> } }'
```

Нужную информацию для выполнения команды можно посмотреть во вьювере (ссылка).

## Перевезти VDisk'и со сломанного/отсутствующего устройства {#removal_from_a_broken_device}

В случае если SelfHeal выключен или не перевозит VDisk'и, данную операцию придется выполнить вручную.

1. Убедиться в мониторинге, что VDisk действительно в нерабочем состоянии.

    Записать fqdn узла, ic-port, путь до VDisk'а, pdiskId

2. Зайти на любой узел кластера

3. Выполнить перевоз VDisk'а

    ```bash
    kikimr admin bs config invoke --proto 'Command { UpdateDriveStatus { HostKey: { Fqdn: "<host>" IcPort: <ic-port>} Path: "<Путь до партлейбла устройства>" PDiskId: <pdisk-id> Status: BROKEN } }'
    ```

## Вернуть PDisk после развоза  {#return_a_device_to_work}

1. Убедиться в мониторинге, что PDisk в рабочем состоянии

    Записать fqdn узла, ic-port, путь до устройства, pdiskId

2. Зайти на любой узел кластера

3. Вернуть PDisk

    ```bash
    kikimr admin bs config invoke --proto 'Command { UpdateDriveStatus { HostKey: { Fqdn: "<host>" IcPort: <ic-pord>} Path: "<Путь до партлейбла устройства>" PDiskId: <pdisk-id> Status: ACTIVE } }'
    ```
