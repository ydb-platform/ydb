# Rolling restart

{{ ydb-short-name }} is a distributed system that supports [rolling restart](../../../../devops/manual/upgrade.md), when database administrators update {{ ydb-short-name }} nodes one by one. This helps keep the {{ ydb-short-name }} cluster up and running during the update process or some {{ ydb-short-name }} configuration changes. However, when a {{ ydb-short-name }} node is being restarted, Hive moves the tables that run on this node to other nodes, and that may lead to increased latencies for queries that are processed by the moving tables.

## Diagnostics

1. Open [Embedded UI](../../../../reference/embedded-ui/index.md).

1. On the **Nodes** tab, see if {{ ydb-short-name }} versions of the nodes differ.

    Also, see if the nodes with the later {{ ydb-short-name }} version have the minimal uptime value.

    ![](_assets/updates.png)

## Recommendations

Wait till all of the nodes of the {{ ydb-short-name }} cluster are updated.
