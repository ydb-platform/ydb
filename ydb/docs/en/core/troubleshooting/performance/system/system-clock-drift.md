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

When monitoring clock skew between nodes, use these **rule-of-thumb thresholds**:

* **Up to ~5 seconds** — investigate skew sources and increase observability.
* **Around 25 seconds and above** — treat as critical; prioritize diagnosis and time alignment.
* **More than 30 seconds** — distributed transactions stop working (see below).

If the system clocks of the nodes running the [coordinator](../../../concepts/glossary.md#coordinator) tablets differ, transaction latencies increase by the time difference between the fastest and slowest system clocks. This occurs because a transaction planned on a node with a faster system clock can only be executed once the coordinator with the slowest clock reaches the same time.

Furthermore, if the system clock drift exceeds 30 seconds, {{ ydb-short-name }} will refuse to process distributed transactions. Before coordinators start planning a transaction, affected [Data shards](../../../concepts/glossary.md#data-shard) determine an acceptable range of timestamps for the transaction. The start of this range is the current time of the mediator tablet's clock, while the 30-second planning timeout determines the end. If the coordinator's system clock exceeds this time range, it cannot plan a distributed transaction, resulting in errors for such queries.

## Diagnostics

To diagnose the system clock drift, use the following methods:

1. Use **Healthcheck** in the [Embedded UI](../../../reference/embedded-ui/index.md):

    1. In the [Embedded UI](../../../reference/embedded-ui/index.md), go to the **Databases** tab and click on the database.

    1. On the **Navigation** tab, ensure the required database is selected.

    1. Open the **Diagnostics** tab.

    1. On the **Info** tab, click the **Healthcheck** button.

        If the **Healthcheck** button displays a `MAINTENANCE REQUIRED` status, the {{ ydb-short-name }} cluster might be experiencing issues, such as system clock drift. Any identified issues will be listed in the **DATABASE** section below the **Healthcheck** button.

    1. To see the diagnosed problems, expand the **DATABASE** section.

        ![](_assets/healthcheck-clock-drift.png)

        The system clock drift problems will be listed under `NODES_TIME_DIFFERENCE`.

    {% note info %}

    For more information, see [Health Check API](../../../reference/ydb-sdk/health-check-api.md)

    {% endnote %}


1. Open the [Interconnect overview](../../../reference/embedded-ui/interconnect-overview.md) page of the [Embedded UI](../../../reference/embedded-ui/index.md). Interconnect metrics (including indicators related to clock skew across nodes) help assess the scope of the issue alongside connectivity latency and error rates.

1. Use such tools as `pssh` or `ansible` to run the command (for example, `date +%s%N`) on all {{ ydb-short-name }} nodes to display the system clock value.

    {% note warning %}

    Network delays between the host that runs `pssh` or `ansible` and {{ ydb-short-name }} hosts will influence the results.

    {% endnote %}

    If you use time synchronization utilities, you can also request their status instead of requesting the current timestamps. For example:

    ```bash
    timedatectl show-timesync --all
    chronyc sources -v
    ```

If you operate external monitoring, also watch transaction latency and gRPC success rates — they may degrade together with interconnect signals when skew is present.

## Recommendations

1. Manually synchronize the system clocks of servers running {{ ydb-short-name }} nodes. For instance, use `pssh` or `ansible` to run the clock sync command across all nodes.

2. Ensure that system clocks on all {{ ydb-short-name }} servers are regularly synchronized using `timesyncd`, `ntpd`, `chrony`, or a similar tool. Use the **same time source policy** for every server in the cluster (the same set of NTP servers or the same NTP hierarchy) and configure **multiple independent** upstream NTP sources.

### NTP configuration examples {#ntp-examples}

Below are examples for **chrony** and **systemd-timesyncd**. Replace server hostnames with values appropriate for your environment and security policy.

**chrony** (`/etc/chrony/chrony.conf`):

```bash
server ntp1.example.net iburst
server ntp2.example.net iburst
server ntp3.example.net iburst
server ntp4.example.net iburst
maxslewrate 10000
local stratum 10
```

**systemd-timesyncd** (snippet, e.g. `/etc/systemd/timesyncd.conf.d/custom.conf`):

```ini
[Time]
NTP=ntp1.example.net ntp2.example.net ntp3.example.net ntp4.example.net
FallbackNTP=ntp1.example.net ntp2.example.net ntp3.example.net ntp4.example.net
```

For production clusters, **chrony** is often preferred due to better behavior under network jitter and step changes.

### Monitoring and alerting {#monitoring}

Configure alerts on clock skew between nodes and related symptoms (Embedded UI health signals and external monitoring). See [Symptoms and thresholds](#symptoms-thresholds) for threshold guidance.

### Emergency recovery {#emergency}

If skew becomes critical, you may need forced synchronization (follow your cluster’s operational procedure):

```bash
# Example for chrony: stop the service and perform a one-shot sync
sudo systemctl stop chrony
sudo chronyd -q 'server ntp1.example.net iburst'
sudo systemctl start chrony
```

For **systemd-timesyncd**, restart the service and force sync according to your Linux distribution documentation.

Suggested sequence on a cluster:

1. Verify NTP servers are correct and reachable.
2. Align time across nodes without divergent per-node configuration.
3. Confirm convergence via Healthcheck and monitoring.
4. Identify the root cause (network, DNS, blocked NTP egress, etc.).

### Prevention {#prevention}

* Periodically review metrics and Healthcheck reports for growing skew.
* Validate NTP fault tolerance (multiple servers; UDP/123 reachability if applicable).
* Keep `chrony` / `timesyncd` configuration consistent across cluster nodes.
