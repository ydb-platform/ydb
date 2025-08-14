# Insufficient memory (RAM)

If [swap](https://en.wikipedia.org/wiki/Memory_paging#Unix_and_Unix-like_systems) (paging of anonymous memory) is disabled on the server running {{ ydb-short-name }}, insufficient memory activates another kernel feature called the [OOM killer](https://en.wikipedia.org/wiki/Out_of_memory), which terminates the most memory-intensive processes (often the database itself). This feature also interacts with [cgroups](https://en.wikipedia.org/wiki/Cgroups) if multiple cgroups are configured.

If swap is enabled, insufficient memory may cause the database to rely heavily on disk I/O, which is significantly slower than accessing data directly from memory.

{% note warning %}

If {{ ydb-short-name }} nodes are running on servers with swap enabled, disable it. {{ ydb-short-name }} is a distributed system, so if a node restarts due to lack of memory, the client will simply connect to another node and continue accessing data as if nothing happened. Swap would allow the query to continue on the same node but with degraded performance from increased disk I/O, which is generally less desirable.

{% endnote %}

Even though the reasons and mechanics of performance degradation due to insufficient memory might differ, the symptoms of increased latencies during query execution and data retrieval are similar in all cases.

Additionally, which components within the  {{ ydb-short-name }} process consume memory may also be significant.

## Diagnostics

1. Determine whether any {{ ydb-short-name }} nodes recently restarted for unknown reasons. Exclude cases of {{ ydb-short-name }} version upgrades and other planned maintenance. This could reveal nodes terminated by OOM killer and restarted by `systemd`.

    1. Open [Embedded UI](../../../reference/embedded-ui/index.md).

    1. On the **Nodes** tab, look for nodes that have low uptime.

    1. Chose a recently restarted node and log in to the server hosting it. Run the `dmesg` command to check if the kernel has recently activated the OOM killer mechanism.

        Look for the lines like this:

        ```plaintext
        [ 2203.393223] oom-kill:constraint=CONSTRAINT_NONE,nodemask=(null),cpuset=user.slice,mems_allowed=0,global_oom,task_memcg=/user.slice/user-1000.slice/session-1.scope,task=ydb,pid=1332,uid=1000
        [ 2203.393263] Out of memory: Killed process 1332 (ydb) total-vm:14219904kB, anon-rss:1771156kB, file-rss:0kB, shmem-rss:0kB, UID:1000 pgtables:4736kB oom_score_adj:0
        ```

    Additionally, review the `ydbd` logs for relevant details.


1. Determine whether memory usage reached 100% of capacity.

    1. Open the **[DB overview](../../../reference/observability/metrics/grafana-dashboards.md#dboverview)** dashboard in Grafana.

    1. Analyze the charts in the **Memory** section.

1. Determine whether the user load on {{ ydb-short-name }} has increased. Analyze the following charts on the **[DB overview](../../../reference/observability/metrics/grafana-dashboards.md#dboverview)** dashboard in Grafana:

    - **Requests** chart
    - **Request size** chart
    - **Response size** chart

1. Determine whether new releases or data access changes occurred in your applications working with {{ ydb-short-name }}.

## Recommendation

Consider the following solutions for addressing insufficient memory:

- If the load on {{ ydb-short-name }} has increased due to new usage patterns or increased query rate, try optimizing the application to reduce the load on {{ ydb-short-name }} or add more {{ ydb-short-name }} nodes.

- If the load on {{ ydb-short-name }} has not changed but nodes are still restarting, consider adding more {{ ydb-short-name }} nodes or raising the hard memory limit for the nodes. For more information about memory management in {{ ydb-short-name }}, see [{#T}](../../../reference/configuration/memory_controller_config.md).
