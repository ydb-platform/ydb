# Clock skew between client hosts and the cluster

System clock skew on a client application host can cause authentication errors — especially when obtaining a token via IAM.

The Healthcheck [`NODES_TIME_DIFFERENCE`](../reference/ydb-sdk/health-check-api.md) compares system time across cluster nodes. It does not check time on client application hosts. Therefore, if Healthcheck reports no issues but authentication errors occur only for some clients, check the system time on those client hosts.

## Symptoms {#symptoms}

* Errors during authentication with a [service account key](../security/authentication.md#iam). In this mode, the application builds a JWT to request a token and sets the current time from the host system clock in that JWT. If the clock is ahead or behind, the IAM token service rejects the JWT.
* If the SDK decides when to refresh a token based on the local clock, a lagging clock may cause the client to send an already expired token to the cluster.
* Errors occur only on some hosts — for example, on new machines or in environments without configured time synchronization.

{% note info %}

`DeadlineExceeded` errors with normal cluster latency are usually unrelated to clock skew: a transport timeout is a wait duration on the client side. In that case, review your timeout settings (see [{#T}](../dev/timeouts.md)).

{% endnote %}

## Diagnostics {#diagnostics}

To check time synchronization on a client host, run:

```bash
chronyc tracking
chronyc sources -v
```

In the `chronyc tracking` output, check the `System time` field (current offset) and `Leap status` (should be `Normal`). If chrony is not installed, use `timedatectl` to verify that the system clock is synchronized.

## Recommendations {#recommendations}

Configure time synchronization on client hosts with `chrony` or `ntpd` and multiple NTP sources from your environment. For configuration examples, see [{#T}](performance/system/system-clock-drift.md#ntp-examples).

The primary fix is to synchronize the system clocks.

{% include [example_clock_skew_internal](_includes/example_clock_skew_internal.md) %}

## See also

* [{#T}](performance/system/system-clock-drift.md)
