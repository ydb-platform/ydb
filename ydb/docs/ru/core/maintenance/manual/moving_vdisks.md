# Перевоз ВДисков

## Увезти один вдиск с блочного устройтства {#moving_vdisk}

Для того чтобы перевезти диск с блочного устройства, надо зайти на ноду по ssh и выполнить следующую команду.

```bash
kikimr admin bs config invoke --proto 'Command { ReassignGroupDisk { GroupId: <ID группы хранения> GroupGeneration: <Поколение группы хранения> FailRealmIdx: <FailRealm> FailDomainIdx: <FailDomain> VDiskIdx: <Номер слота> } }'
```

Нужную информацию для выполнения команды можно посмотреть во вьювере (ссылка).

## Перевезти вдиски со сломанного/отсутствующего устройства {#removal_from_a_broken_device}

В случае если SelfHeal выключен или не перевозит вдиски, данную операцию придется выполнить вручную.

1. Убедиться в мониторинге, что диск действительно в нерабочем состоянии.  

    Записать fqdn узла, ic-port, путь до диска, pdiskId

2. Зайти на любой узел кластера

3. Выполнить перевоз диска

    ```bash
    kikimr admin bs config invoke --proto 'Command { UpdateDriveStatus { HostKey: { Fqdn: "<Xост>" IcPort: <IC Порт>} Path: "<Путь до партлейбла устройства>" PDiskId: <ID ПДиска> Status: BROKEN } }'
    ```

## Вернуть диск после развоза  {#return_a_device_to_work}

1. Убедиться в мониторинге, что диск в рабочем состоянии  

    Записать fqdn узла, ic-port, путь до диска, pdiskId

2. Зайти на любой узел кластера

3. Вернуть диск

    ```bash
    kikimr admin bs config invoke --proto 'Command { UpdateDriveStatus { HostKey: { Fqdn: "<Xост>" IcPort: <IC Порт>} Path: "<Путь до партлейбла устройства>" PDiskId: <ID ПДиска> Status: ACTIVE } }'
    ```