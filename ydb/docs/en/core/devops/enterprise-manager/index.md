# {{ ydb-short-name }} Enterprise Manager

{{ ydb-short-name }} Enterprise Manager (hereinafter referred to as YDB EM) is a tool for centralized management of {{ ydb-short-name }} clusters through a web interface and API.

{% note info %}

Deploying {{ ydb-short-name }} clusters is possible without YDB EM — using [Ansible](../deployment-options/ansible/index.md), [Kubernetes](../deployment-options/kubernetes/index.md), or [manually](../deployment-options/manual/index.md). YDB EM provides a convenient web interface on top of existing clusters.

{% endnote %}

## Purpose {#purpose}

YDB EM connects to existing {{ ydb-short-name }} clusters and provides a graphical interface and API for the following tasks:

* centralized access to databases and {{ ydb-short-name }} clusters from a single interface;
* managing dynamic nodes of a cluster — starting, stopping, and scaling;
* managing databases — creating, deleting, and modifying parameters;
* monitoring cluster and node status;
* managing resources (CPU, RAM) allocated to dynamic nodes;
* advisor — diagnostics and recommendations for resolving the most common issues;
* advanced SQL editor for running queries against databases;
* AI assistant for working with {{ ydb-short-name }}.

{% note warning %}

YDB EM does not deploy {{ ydb-short-name }} clusters. Before using YDB EM, the cluster must be deployed using one of the [deployment options](../deployment-options/index.md).

{% endnote %}

## Architecture {#architecture}

YDB EM consists of three components:

* **Gateway** — web interface and API backend. Accepts requests from users (via a browser or API) and communicates with the Control Plane and the YDB EM database.
* **Control Plane (CP)** — coordinates cluster management. Receives commands from Gateway, stores configuration in the YDB EM database, and dispatches tasks to agents.
* **Agent** — runs on each {{ ydb-short-name }} cluster host where dynamic nodes operate. The agent executes Control Plane commands: starts and stops {{ ydb-short-name }} node processes, monitors their state, and reports information about available host resources.

To store its own metadata (cluster configuration, node state), YDB EM uses a {{ ydb-short-name }} database — it can reside in the same cluster that EM manages.

### Interaction diagram {#interaction-diagram}

```mermaid
flowchart LR
    User[User] -- "HTTP/HTTPS" --> Gateway

    subgraph YDBEM [YDB EM]
        Gateway[Gateway]
        CP[Control Plane]
        EMDB[(YDB EM DB)]
        Gateway -- "gRPC" --> CP
        Gateway -- "gRPC" --> EMDB
        CP -- "gRPC" --> EMDB
    end

    subgraph Cluster [YDB Cluster]
        subgraph Host1 [Host 1]
            Node1[YDB Node]
            Agent1[Agent]
        end
        subgraph Host2 [Host 2]
            Node2[YDB Node]
            Agent2[Agent]
        end
        subgraph Host3 [Host N]
            Node3[YDB Node]
            Agent3[Agent]
        end
    end

    CP -- "gRPC" --> Agent1
    CP -- "gRPC" --> Agent2
    CP -- "gRPC" --> Agent3
    Agent1 -- "gRPC" --> CP
    Agent2 -- "gRPC" --> CP
    Agent3 -- "gRPC" --> CP
```

The user interacts with Gateway through a browser or API. Gateway forwards requests to the Control Plane, which coordinates the work of agents on cluster hosts. Agents manage {{ ydb-short-name }} node processes and report host status.

## Key topics {#materials}

- [{#T}](initial-deployment.md)
