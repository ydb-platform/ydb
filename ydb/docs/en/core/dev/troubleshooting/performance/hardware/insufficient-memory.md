# Insufficient memory (RAM)

If [swap](https://en.wikipedia.org/wiki/Memory_paging#Unix_and_Unix-like_systems) (paging of anonymous memory) is disabled on the server running {{ ydb-short-name }}, insufficient memory activates another kernel feature called the [OOM killer](https://en.wikipedia.org/wiki/Out_of_memory), which terminates the most memory-intensive processes (often the database itself). This feature also interacts with [cgroups](https://en.wikipedia.org/wiki/Cgroups) if multiple cgroups are configured.

If swap is enabled, insufficient memory may cause the database to rely heavily on disk I/O, which is significantly slower than accessing data directly from memory.

{% note info %}

It's recommended to disable swap on {{ ydb-short-name }} servers.

{% endnote %}

Even though the reasons and mechanics of performance degradation due to insufficient memory might differ, the symptoms of increased latencies during query execution and data retrieval are similar in all cases.

Additionally, which components within the  {{ ydb-short-name }} process consume memory may also be significant.

## Diagnostics

1. Determine whether any {{ ydb-short-name }} nodes recently restarted for unknown reasons. Exclude cases of {{ ydb-short-name }} upgrades.

    {% note info %}

    This step might reveal nodes terminated by OOM killer and restarted by {{ ydb-short-name }}.

    {% endnote %}

    1. Open [Embedded UI](../../../../reference/embedded-ui/index.md).

    1. On the **Nodes** tab, look for nodes that have low uptime.

    1. Log in to the recently restarted nodes and run the `dmesg` command to diagnose the reasons for the restart.


1. Determine whether memory usage reached 100%.

    1. Open the **DB overview** dashboard in Grafana.

    1. Analyze the charts in the **Memory** section.

1. Determine whether the user load on {{ ydb-short-name }} has increased. Analyze the following charts on the **DB overview** dashboard in Grafana:

    - **Requests** chart
    - **Request size** chart
    - **Response size** chart

1. Determine whether new releases or data usage changes occurred in your applications.

## Recommendation

Consider the following solutions to the problem of insufficient memory:

- If the load on {{ ydb-short-name }} increased because of new usage patterns or higher volume of queries, try to optimize the application to reduce the load on {{ ydb-short-name }} or add more {{ ydb-short-name }} nodes.

- If the load on {{ ydb-short-name }} has not changed, but {{ ydb-short-name }} nodes still restart, consider adding more {{ ydb-short-name }} nodes or raising the hard memory limit available to {{ ydb-short-name }} nodes.
