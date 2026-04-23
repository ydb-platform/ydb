# YDB Enterprise Manager Overview

YDB Enterprise Manager (YDB EM) is a system for managing YDB clusters, handling resources, databases, and dynamic slots on hosts.

## Components

YDB EM consists of the following main components:

- **Gateway**: Provides the UI and the API backend for the UI.
- **Control Plane (CP)**: Responsible for controlling the YDB cluster, managing resources, and configuring databases.
- **Agent**: A system for controlling dynamic slots on a host.

## Architecture

The diagram below illustrates the high-level architecture of YDB EM and its interactions with the browser and YDB clusters.

```mermaid
flowchart LR
    User[fa:fa-user User] --> Browser
    Browser --http/https--> meta[YDB EM Gateway]
    ydb-host01-ydb --metainfo--> meta
    ydb-host01-agent --get dynnode--> CP
    ydb-host02-agent --get dynnode--> CP
    ydb-host03-agent --get dynnode--> CP

    subgraph YDBEM
        meta --grpc/grpcs--> YDB(fa:fa-database YDB EM DB)
        meta --grpc/grpcs--> CP[YDB EM CP]
        CP --grpc/grpcs--> YDB
    end


    subgraph YDBCluster1
        subgraph Node01
            ydb-host01-ydb[YDB Node 1]
            ydb-host01-agent[YDB EM CP agent1]
        end
        subgraph Node02
            ydb-host02-ydb[YDB Node 2]
            ydb-host02-agent[YDB EM CP agent2]
        end
        subgraph Node03
            ydb-host03-ydb[YDB Node 3]
            ydb-host03-agent[YDB EM CP agent3]
        end
    end
```

## Usage

<!-- TODO: Add more detailed usage instructions or overview information here if necessary -->

To access the YDB Enterprise Manager UI, open the following URL in your browser:

`https://<fqdn>:8789/ui/clusters`

*Note*: Ensure you replace `<fqdn>` with the Fully Qualified Domain Name of any host from the `ydb_em` host group (for example, `ydb-node01.ru-central1.internal`).
