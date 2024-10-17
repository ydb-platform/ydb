# DC Outage

To determine if one of the data centers of the {{ ydb-short-name }} cluster is not available, follow these steps:

1. Open [Embedded UI](../../../../../reference/embedded-ui/index.md).

1. On the **Nodes** tab, analyze the [health indicators](../../../../../reference/embedded-ui/ydb-monitoring.md#colored_indicator) in the **Host** columns and **DC** column values.

    ![](../_assets/cluster-nodes.png)

    If all of the nodes in one of the DCs (data centers) are not available, this data center is most likely offline.
