# Frequent tablet transfers between nodes

{{ ydb-short-name }} automatically balances the load by transferring tablets from overloaded nodes to other nodes. This process is managed by [Hive](*hive). When Hive moves tablets, queries to those tablets suffer higher latencies.

## Diagnostics

<!-- The include is added to allow partial overrides in overlays  -->
{% include notitle [#](_includes/tablets-moved.md) %}


[*hive]: A Hive is a system tablet responsible for launching and managing other tablets. Its responsibilities include moving tablets between nodes in case of node failure or overload.
