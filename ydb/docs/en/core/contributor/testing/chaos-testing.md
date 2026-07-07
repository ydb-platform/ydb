# Chaos Testing

Chaos testing is a methodology for verifying the resilience of {{ ydb-short-name }} by deliberately injecting failures into a running cluster. The goal is to ensure that {{ ydb-short-name }} correctly survives real-world failures: node losses, network partitions, disk problems, and other abnormal situations. The **[Nemesis](https://github.com/ydb-platform/ydb/tree/main/ydb/tests/stability/nemesis)** tool is responsible for injecting this chaos.

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
- **BSController** — distributed storage controller
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

- Stopping all nodes in a single [pile](../../concepts/glossary.md#pile)
- Network isolation of a [pile](../../concepts/glossary.md#pile)

## Integration with Stress Testing

Chaos testing is typically run in conjunction with stress testing workloads from the [ydb/tests/stress](https://github.com/ydb-platform/ydb/tree/main/ydb/tests/stress) directory. This combination ensures that the cluster is tested under load conditions while experiencing various failure scenarios.

## How Verification Works

While failures are being injected, the system checks two [properties](https://en.wikipedia.org/wiki/Safety_and_liveness_properties) on demand:

- **Liveness** — the cluster remains available and continues to process requests
- **Safety** — no signs of data correctness violations or internal system invariant breaches appear in cluster logs and metrics

Failures are injected automatically on a schedule, and check results are aggregated and available for analysis.

## The Nemesis Tool

{{ ydb-short-name }} uses the **Nemesis** tool for chaos testing — a fault injection application located in the [YDB repository on GitHub](https://github.com/ydb-platform/ydb/tree/main/ydb/tests/stability/nemesis). It is deployed directly on the nodes of the cluster under test and manages fault injection according to a configured schedule.

{% note warning %}

Nemesis only works with {{ ydb-short-name }} clusters that were deployed using the [`ydbd_slice`](https://github.com/ydb-platform/ydb/tree/main/ydb/tools/ydbd_slice) utility.

{% endnote %}

### Installation

Deploy Nemesis to your cluster:

```bash
# Single-file config (cluster.yaml contains both hosts and database template)
nemesis install --yaml-config-location /path/to/cluster.yaml

# Two-file config (separate cluster.yaml and databases.yaml)
nemesis install \
    --yaml-config-location /path/to/cluster.yaml \
    --database-config-location /path/to/databases.yaml
```

The first host in the cluster configuration becomes the orchestrator; all other hosts become agents. Services are deployed as systemd units and started automatically.

The primary way to observe a Nemesis run and inspect its results is the web UI served by the orchestrator. Open the URL printed at the end of `nemesis install` in a browser; by default it is available at:

```bash
http://<orchestrator_host>:31434/static/index.html
```

The UI displays:

- **Active faults** — currently injected faults and their execution logs, grouped by category (network, node, tablet, disk, datacenter, pile)
- **Schedules** — fault types registered for automatic injection, their configured intervals, and the next scheduled run
- **Manual controls** — buttons to inject individual faults on demand
- **Execution history** — past fault injections with timestamps and target hosts
- **Liveness checks** — cluster-wide health checks run by the orchestrator
- **Safety checks** — violation detectors over the local logs for the last 24 hours

The same data is available via the HTTP API on the orchestrator if you need to scrape it programmatically.


### Interpreting Results

A Nemesis run is judged against two [properties](https://en.wikipedia.org/wiki/Safety_and_liveness_properties): **liveness** and **safety**. The pass/fail criteria are:

- **Pass** — throughout the run, all liveness checks report the cluster as healthy and all safety wardens return empty violation lists
- **Fail** — at least one liveness check reported the cluster as unavailable (cluster did not recover after a fault within the expected window), **or** at least one safety warden returned a non-empty list of violations (for example, errors or assertions in cluster logs, schemeshard inconsistencies, datashard invariant breaches)

In the UI, healthy checks appear in the violations list; failed checks display the violation messages produced by the corresponding warden.

To investigate a problem, open the failing check in the UI and read the listed violation messages.

For the meaning of individual checks and the exact patterns each warden looks for, see the [Nemesis README](https://github.com/ydb-platform/ydb/blob/main/ydb/tests/stability/nemesis/README.md).


### Stopping Services

Stop all Nemesis services on the cluster:

```bash
nemesis stop --yaml-config-location /path/to/cluster.yaml
```

## Extending Nemesis

To add a new chaos type (fault) or a new safety/liveness check, see the [README](https://github.com/ydb-platform/ydb/blob/main/ydb/tests/stability/nemesis/README.md). It describes how to implement and register new fault runners, planners, and warden classes.

