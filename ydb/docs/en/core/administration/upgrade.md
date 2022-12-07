# {{ ydb-short-name }} updating

{{ ydb-short-name }} is a distributed system that supports rolling restart without downtime or performance degradation.

## Update procedure {#upgrade_order}

The basic use case is rolling updates:

1. Updating storage nodes.
2. Updating dynamic nodes.

The shutdown and startup process is described on the [Safe restart and shutdown of nodes](../maintenance/manual/node_restarting.md) page. You must update {{ ydb-short-name }} nodes one by one and monitor the cluster status after each step in [{{ ydb-short-name }} Monitoring](../maintenance/embedded_monitoring/ydb_monitoring.md).

## Version compatibility {#version-compatability}

All minor versions within a major version are compatible for updates. Major versions are consistently compatible. To update to the next major version, you must first update to the latest minor release of the current major version. For example:

* X.Y.* → X.Y.*: Update is possible, all minor versions within a single major version are compatible.
* X.Y.Z (the latest available version in X.Y.*) → X.Y+1.* : Update is possible, major versions are consistent.
* X.Y.* → X.Y+2.*: Update is impossible, major versions are inconsistent.
* X.Y.* → X.Y-2.*: Update is impossible, major versions are inconsistent.

A list of available versions can be found on the [download page](https://ydb.tech/en/docs/downloads/).