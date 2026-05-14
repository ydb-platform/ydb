# Chaos Testing

Chaos testing is a methodology for verifying the resilience of {{ ydb-short-name }} by deliberately injecting failures into a running cluster. The goal is to ensure that {{ ydb-short-name }} correctly survives real-world failures: node losses, network partitions, disk problems, and other abnormal situations. The **Nemesis** tool is responsible for injecting this chaos.

## What Is Tested

Chaos testing verifies cluster behavior under the following types of failures:

### Network Failures

- Node network isolation (blocking incoming and outgoing traffic)
- System time skew on nodes

### Node Failures

- Forceful termination of cluster node processes
- Stopping and restarting nodes
- Suspending node processes

### Tablet Failures

Tablets are the primary computational units of {{ ydb-short-name }}. Resilience is verified by forcefully terminating various tablet types:

- **Coordinator** — distributed transaction coordinator
- **Hive** — tablet placement manager
- **BsController** — distributed storage controller
- **SchemeShard** — schema manager
- **DataShard** — data storage tablets
- **Mediator** — transaction mediator
- **PersQueue** — message queue tablets
- Other system tablets

Tablet rebalancing between nodes via Hive is also tested.

### Disk Failures

- Safely taking a disk out of service on a node
- Cleaning up disks on nodes

### Multi-Datacenter Cluster Scenarios

- Stopping all nodes in a single datacenter
- Network isolation of a datacenter

### Bridge Mode Cluster Scenarios

- Stopping all nodes in a single bridge pile
- Network isolation of a bridge pile

## How Verification Works

While failures are being injected, the system continuously checks two aspects:

- **Liveness** — the cluster remains available and continues to process requests
- **Safety** — no signs of data correctness violations or internal system invariant breaches appear in cluster logs and metrics

Failures are injected automatically on a schedule, and check results are aggregated and available for analysis.

## The Nemesis Tool

{{ ydb-short-name }} uses the **Nemesis** tool for chaos testing — a fault injection application located at `ydb/tests/stability/nemesis`. It is deployed directly on the nodes of the cluster under test and manages fault injection according to a configured schedule.
