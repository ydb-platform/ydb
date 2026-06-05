# System clock drift

Synchronized clocks are critical for distributed databases. If system clocks on the {{ ydb-short-name }} servers drift excessively, distributed transactions will experience increased latencies.

{% note alert %}

It is important to keep system clocks on the {{ ydb-short-name }} servers in sync, to avoid high latencies.

{% endnote %}

## Symptoms and thresholds {#symptoms-thresholds}

Typical symptoms of clock skew across nodes:

* **Slower distributed transactions** — end-to-end latency can increase roughly by the amount of clock skew (a node with a “faster” clock waits for coordination with nodes that lag behind).
* **Timeouts and deadline issues** — skew can cause spurious client/server timeouts and deadline failures.
* **Authentication issues** — large skew can break token validity windows and related checks.

When monitoring clock skew between nodes, use these thresholds (typical levels for cluster observability):

* **Less than 1 millisecond** — normal level for routine cluster operation.
* **1—5 milliseconds** — a range where it already makes sense to look for the cause of desynchronization and increase observability.
* **More than 5 milliseconds** — critical level: urgent diagnosis and time alignment are required.
* **25 milliseconds and above** — growing performance degradation; users see widespread latency issues and timeouts on distributed transactions.
* **30 seconds** — distributed transactions stop working (see below).

If the system clocks of the nodes running the [coordinator](../../../concepts/glossary.md#coordinator) tablets differ noticeably from one another, transaction latencies increase by the time difference between the fastest and slowest system clocks. This occurs because a transaction planned on a node with a faster system clock can only be executed once the node with the slowest clocks reaches the same time.

Furthermore, if the system clock drift exceeds 30 seconds, {{ ydb-short-name }} will refuse to process distributed transactions. Before coordinators start planning a transaction, affected [data shards](../../../concepts/glossary.md#data-shard) determine an acceptable range of timestamps for the transaction. The start of this range is the current system time of the mediator tablet, and the end is defined by the 30-second planning timeout. If the coordinator's system clock falls outside this time range, it cannot plan a distributed transaction, resulting in errors for such queries.

## Diagnostics

To diagnose system clock drift on {{ ydb-short-name }} servers, use the following methods:

1. Use **Healthcheck** in the [Embedded UI](../../../reference/embedded-ui/index.md):

    1. In the [Embedded UI](../../../reference/embedded-ui/index.md), go to the **Databases** tab and click on the database.

    1. On the **Navigation** tab, ensure the required database is selected.

    1. Open the **Diagnostics** tab.

    1. On the **Info** tab, click the **Healthcheck** button.

        If the **Healthcheck** button displays a `MAINTENANCE REQUIRED` status, the {{ ydb-short-name }} cluster might be experiencing issues, such as system clock drift. Any identified issues will be listed in the **DATABASE** section below the **Healthcheck** button.

    1. To see the diagnosed problems, expand the **DATABASE** section.

        ![](_assets/healthcheck-clock-drift.png)

        System clock drift problems are listed under `NODES_TIME_DIFFERENCE`.

    {% note info %}

    For more information, see [{#T}](../../../reference/ydb-sdk/health-check-api.md)

    {% endnote %}


1. Open the [Interconnect overview](../../../reference/embedded-ui/interconnect-overview.md) page in the [Embedded UI](../../../reference/embedded-ui/index.md). Interconnect metrics (including indicators related to clock skew across nodes) help assess the scope of the issue alongside the overall picture of connectivity latency and errors.

    {% note info %}

    An increase in system clock skew according to interconnect monitoring (as shown in the Embedded UI and collected via cluster metrics) may be caused by actual clock drift, exhaustion of resources in the interconnect CPU pool, or network equipment overload.

    {% endnote %}

1. Use such tools as `pssh` or `ansible` to run the command (for example, `date +%s%N`) on all {{ ydb-short-name }} nodes to display the system clock value.

    {% note warning %}

    Network delays between the host that runs `pssh` or `ansible` and {{ ydb-short-name }} hosts will influence the results.

    {% endnote %}

    If you use time synchronization utilities, you can also request their status instead of requesting the current timestamps. For example:

    ```bash
    chronyc sources -v
    ```

It is also useful to review cluster monitoring metrics (if you collect them): transaction latency and gRPC success rates may degrade together with interconnect indicators when skew is present (in particular, the `interconnect.ClockSkewMicrosec` metric).

## Recommendations

1. Manually synchronize the system clocks of servers running {{ ydb-short-name }} nodes. For instance, use `pssh` or `ansible` to run the clock sync command across all nodes.

2. Ensure that system clocks on all {{ ydb-short-name }} servers are regularly synchronized using `ntpd`, `chrony`, or a similar tool. Use the same logical time source for every server in the cluster (the same set of NTP servers or the same NTP hierarchy) and configure multiple independent upstream NTP sources.

{% note warning %}

Using the **systemd-timesyncd** service is not recommended, because this type of NTP client does not provide sufficient accuracy for system time synchronization. Use other tools instead, such as `chrony` or `ntpd`.

{% endnote %}

### NTP configuration examples {#ntp-examples}

Below are example settings for `chrony`. Replace the server list with values appropriate for your environment and security policy.

**chrony** (`/etc/chrony/chrony.conf`):

```bash
server ntp1.example.net iburst
server ntp2.example.net iburst
server ntp3.example.net iburst
server ntp4.example.net iburst
maxslewrate 10000
local stratum 10
```

### Monitoring and alerting {#monitoring}

Configure alerts on clock skew between nodes and related symptoms (including Embedded UI data and external monitoring). See [Symptoms and thresholds](#symptoms-thresholds) for threshold guidance.

### Emergency recovery {#emergency}

If skew becomes critical, you may need forced synchronization (perform this deliberately and according to your cluster’s agreed procedure):

```bash
# Example for chrony: stop the service and perform a one-shot sync
sudo systemctl stop chrony
sudo chronyd -q 'server ntp1.example.net iburst'
sudo systemctl start chrony
```

Suggested sequence on a cluster:

1. Verify NTP servers are correct and reachable.
2. Align time across nodes (without divergent per-node configuration).
3. Confirm convergence via Healthcheck and monitoring.
4. Identify the root cause of the synchronization failure (network, DNS, blocked NTP egress, etc.).

### Prevention {#prevention}

* Periodically review metrics and Healthcheck reports for growing skew.
* Validate NTP fault tolerance (multiple NTP servers; UDP/123 reachability if applicable).
* Keep the configuration of the NTP client you use consistent across all cluster nodes.
