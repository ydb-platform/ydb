# Overview of metadata distribution services

**Metadata distribution services** are three interconnected subsystems of the {{ ydb-short-name }} cluster that deliver service information between nodes: **StateStorage**, **Board**, and **SchemeBoard**. All of them are built on a distributed quorum service with deterministic replica placement.

This article provides an overview of the purpose of the services without diving into internal identifiers and core mechanisms. A detailed description for core developers is in the [Metadata distribution subsystems](../../contributor/metadata-distribution.md) section. Configuration instructions are in Configuring metadata distribution subsystems.

## Why metadata distribution services are needed {#why}

The {{ ydb-short-name }} cluster is a distributed system in which millions of [tablets](../glossary.md#tablet) can run simultaneously on thousands of nodes. Cluster components need to know:

- where the leader of a specific tablet runs and how to reach it (**StateStorage**).
- which nodes provide services, such as connection points for clients (**Board**).
- what the current database schema is (**SchemeBoard**).

Distributing this data from a single cluster node is bad — it creates high load and problems when that node fails. Therefore, metadata is distributed across many cluster nodes using three specialized subsystems.

## StateStorage {#state-storage}

**StateStorage** stores the current state of tablets: who is the leader now, the generation and step of leader election, and the list of replicas. By tablet id, through this service you can find out the current actor id, which allows interacting with it through the actor system.

{% note info %}

Data in StateStorage is **volatile**: it is stored in the memory of replicas and is restored on restart. This is not a long-term storage.

{% endnote %}

## Board {#board}

**Board** is a service for publishing and subscribing to metadata in the format «path → set of records». The main use case is storing database [endpoints](../connect.md#endpoint): nodes publish addresses, clients and other components subscribe to changes.

## SchemeBoard {#scheme-board}

**SchemeBoard** distributes schema metadata: tables, indexes, topics, access rights. Database nodes use it as a schema cache to avoid contacting [SchemeShard](../glossary.md#scheme-shard) on every query.

## Comparison of services {#comparison}

| Characteristic | StateStorage | Board | SchemeBoard |
| --- | --- | --- | --- |
| **Purpose** | Tablet state and leadership | Publishing service metadata | Schema distribution |
| **Data type** | Tablet leader state | Path → payload pairs | Schema object descriptions |
| **Key** | Tablet identifier | Path (string) | Path to schema object |
| **Main consumers** | Components interacting with tablets | gRPC proxies, clients | Database nodes |

## General principle {#common-principle}

All three services use one architectural approach: a record is addressed by key, the set of replicas for a key is computed deterministically, and operations are performed by quorum. This ensures scalability and fault tolerance during rolling restarts and rack failures.

Details (replica rings, quorum, configuration changes, placement across failure domains) are described in the article [Metadata distribution subsystems](../../contributor/metadata-distribution.md).

## Related materials {#related}

- [Metadata distribution subsystems](../../contributor/metadata-distribution.md) — a detailed description for core contributors.
- Configuring metadata distribution subsystems.
- [Self Heal State Storage](../../maintenance/manual/selfheal_statestorage.md).
- [Bridge mode](../bridge.md).
- [Cluster topology](../topology.md).
- [Glossary](../glossary.md).
