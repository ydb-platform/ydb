# System clock drift

Synchronized clocks are critical for distributed databases. If system clocks on the {{ ydb-short-name }} servers start to drift too much, distributed transactions might be processed with higher latencies. In some cases {{ ydb-short-name }} might even fail to process distributed transactions and return errors.

If a {{ ydb-short-name }} cluster contains multiple [coordinators](../../../../concepts/glossary.md#coordinator), planned transactions are merged by [mediators](../../../../concepts/glossary.md#mediator).

If the system clocks of the nodes that run coordinator tablets differ, transaction latencies grow by the time difference between the fastest and the slowest system clocks. The latency grows because a transaction planned on a node, which system clock is faster, will be executed only after the coordinator with the slowest wall clock reaches that time.

Moreover, if the system clock drift exceeds 30 seconds, {{ ydb-short-name }} might fail to process distributed transactions. Before coordinators start planning a transaction, affected DataShards determine an acceptable range of timestamps for the transaction. The start of the time range is the current clock time of the mediator tablet. The end time is determined by the planning timeout of 30 seconds. If the system clock of a given coordinator is past this time range of the DataShard, the coordinator cannot plan a distributed transaction for the DataShard, and such queries will always end with an error.

## Diagnostics

To diagnose the system clock drift, use one of the following methods:

- Open the [Interconnect overview](../../../../reference/embedded-ui/interconnect-overview.md) page of the [Embedded UI](../../../../reference/embedded-ui/index.md).

- Use such tools as `pssh` or `ansible` to run the command on all {{ ydb-short-name }} nodes to display the system clock value.

## Recommendations

1. Sync the system clocks of {{ ydb-short-name }} nodes manually. For example, you can use `pssh` or `ansible` to run the clock sync command on all of the nodes.

1. Ensure that system clocks on all of the {{ ydb-short-name }} servers are synced by `ntpd` or `chrony`. It's recommended to use the same time source for all servers in the {{ ydb-short-name }} cluster.
