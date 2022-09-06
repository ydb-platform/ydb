# Cluster extension

You can expand the {{ ydb-short-name }} cluster by adding new nodes to the cluster configuration.

1. Specify the parameters of additional nodes in the file `names.txt ` NameserviceConfig configuration:

    ```protobuf
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

1. [Update the NameserviceConfig](./cms.md) via CMS.

1. Add new nodes to DefineBox

    ```protobuf
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

1. Run the command:

    ```protobuf
    kikimr -s <endpoint> admin bs config invoke --proto-file DefineBox.txt
    ```
