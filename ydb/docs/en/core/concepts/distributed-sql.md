# Distributed SQL

## What's Distributed SQL?

**Distributed SQL** databases like {{ ydb-short-name }} are designed to handle large-scale workloads by distributing data across multiple nodes while maintaining transactional guarantees. They typically leverage a distributed consensus algorithm such as [Raft](https://en.wikipedia.org/wiki/Raft_(algorithm)), [Paxos](https://en.wikipedia.org/wiki/Paxos_(computer_science)), or [Calvin](https://cs.yale.edu/homes/yu-ren/Calvin_Sigmod12.pdf) to ensure consistency and fault tolerance, allowing the system to continue operating correctly as a whole even in the presence of node failures.

## Core Concepts of Distributed SQL

A **distributed SQL database** is a relational system that:

* Presents a **single logical database** even though data is physically distributed across multiple nodes
* Guarantees **ACID transactions** across shards with **strong consistency**
* **Automatically shards** and **replicates** data for balanced load and fault tolerance
* Supports **SQL** queries for compatibility with existing tools and skills

Typical **Distributed SQL** database mechanisms include:

* **Sharding** — tables transparently split by primary key to distribute load across nodes
* **Distributed transactions** — the cluster coordinates multi-shard and cross-table writes with transactional guarantees
* **Transparent routing** — the cluster routes each query within the cluster to the appropriate components and data locations
* **Online rebalancing** — the cluster moves shards between nodes to adjust to workload while the database remains fully operational
* **Scale-out** — operators add or remove servers as necessary and the cluster automatically re-distributes data transparently
* **Parallel execution** — queries are pushed to involved shards and partial results are merged to provide the final answer
* **Geo-distribution** – clusters can [span availability zones](./topology.md)
* **Rolling upgrades and downgrades** — nodes can restart gradually and have different software versions but still work together as a cluster that continuously serves queries

These properties allow **Distributed SQL DBMS** to manage large datasets under high load without downtime.

## Choosing a Distributed SQL Configuration

When planning a {{ ydb-short-name }} deployment, consider:

1. **Capacity planning** — estimate future throughput and data volume requirements
2. **Consistency needs** — analyze the required consistency guarantees to spend less time working around consistency anomalies on the application level
3. **Operational model** — decide between self-managed or managed cloud deployment options
4. **Feature requirements** — verify the required functionality is supported


## Conclusion

Distributed SQL combines relational database features with distributed systems principles. {{ ydb-short-name }} delivers ACID guarantees, transparent sharding, and high availability through its architecture. The system is designed to support business-critical workloads requiring scale, consistency, reliability, and security. {{ ydb-short-name }} goes beyond what you could expect from a typical **Distributed SQL** DBMS by providing [AI capabilities](ai-database.md) and serving as a [universal data platform](universal-database.md). The system executes queries with [serializable isolation](./transactions.md) and [MVCC](./mvcc.md) by default.