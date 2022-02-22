# Расширение кластера {#expand_cluster}

1) Добавить в конфиг NameserviceConfig в файл names.txt информацию о новых узлах

    ```
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

2) Обновить конфиг NameserviceConfig через CMS

3) Добавить новые узлы в DefineBox

    Пример протофайла для DefineBox

    ```
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

    Применение команды

    ```
    kikimr -s <ендпоинт> admin bs config invoke --proto-file DefineBox.txt
    ```
