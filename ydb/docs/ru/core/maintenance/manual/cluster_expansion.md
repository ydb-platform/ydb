# Расширение кластера

Вы можете расширить кластер {{ ydb-short-name }}, добавив новые узлы в конфигурацию кластера.

1. Укажите параметры дополнительных узлов в файле `names.txt` конфигурации NameserviceConfig:

    ```protobuf
    Node {
        NodeId: 1
        Port: <ic-port>
        Host: "<существующий-хост>"
        InterconnectHost: "<существующий-хост>"
        Location {
            DataCenter: "DC1"
            Module: "M1"
            Rack: "R1"
            Unit: "U1"
        }
    }
    Node {
        NodeId: 2
        Port: <ic-port>
        Host: "<новый-хост>"
        InterconnectHost: "<новый-хост>"
        Location {
            DataCenter: "DC1"
            Module: "M2"
            Rack: "R2"
            Unit: "U2"
        }
    }
    ClusterUUID: "<UUID-кластера>"
    AcceptUUID: "<UUID-кластера>"
    ```

1. [Обновите конфигурацию](./cms.md) NameserviceConfig с помощью CMS.

1. Добавьте новые узлы в DefineBox:

    ```protobuf
    Command {
        DefineHostConfig {
            HostConfigId: 1
            Drive {
                Path: "<путь-до-устройства>"
                Type: SSD
                PDiskConfig {
                    ExpectedSlotCount: 2
                }
            }
        }
    }
    Command {
        DefineBox {
            BoxId: 1
            Host {
                Key {
                    Fqdn: "<существующий-хост>"
                    IcPort: <ic-port>
                }
                HostConfigId: 1
            }
            Host {
                Key {
                    Fqdn: "<новый-хост>"
                    IcPort: <ic-port>
                }
                HostConfigId: 1
            }
        }
    }
    ```

1. Выполните команду:

    ```protobuf
    kikimr -s <endpoint> admin bs config invoke --proto-file DefineBox.txt
    ```
