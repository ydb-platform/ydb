# Cluster extension {#expand_cluster}

1) Add information about new nodes to NameserviceConfig in the names.txt file

    ```
    Node {
        NodeId: 1
        Port: <ic-port>
        Host: "<existing-host>"
        InterconnectHost: "<existing-host>"
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
        Host: "<new-host>"
        InterconnectHost: "<new-host>"
        Location {
            DataCenter: "DC1"
            Module: "M2"
            Rack: "R2"
            Unit: "U2"
        }
    }
    ClusterUUID: "<cluster-UUID>"
    AcceptUUID: "<cluster-UUID>"
    ```

2) Update the NameserviceConfig via CMS

3) Add new nodes to DefineBox

    Sample protobuf for DefineBox

    ```
    Command {
        DefineHostConfig {
            HostConfigId: 1
            Drive {
                Path: "<device-path>"
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
                    Fqdn: "<existing-host>"
                    IcPort: <ic-port>
                }
                HostConfigId: 1
            }
            Host {
                Key {
                    Fqdn: "<new-host>"
                    IcPort: <ic-port>
                }
                HostConfigId: 1
            }
        }
    }
    ```

    Using the command

    ```
    kikimr -s <endpoint> admin bs config invoke --proto-file DefineBox.txt
    ```
