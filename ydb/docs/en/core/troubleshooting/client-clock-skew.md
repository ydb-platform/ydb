# Clock skew between client hosts and the cluster

System clock skew on the host where a client application (SDK or CLI) runs can cause authentication errors. This is especially common when obtaining a token via IAM with a [service account key](../security/authentication.md#iam) (**Service Account Key**), where the token includes time from the host.

The Healthcheck [NODES_TIME_DIFFERENCE](../reference/ydb-sdk/health-check-api.md#nodes_time_difference) compares system time across cluster nodes. It does not check time on client application hosts. Therefore, if Healthcheck reports no issues but authentication errors occur only for some clients, check the system time on those client hosts.

## Symptoms {#symptoms}

- **Errors during authentication with a [service account key](../security/authentication.md#iam)**. The client (SDK or CLI) builds a [JWT](https://www.rfc-editor.org/rfc/rfc7519) to request a token and sets the current time from the host system clock in that JWT. If the clock is ahead or behind, IAM may reject the token request.
- **Authentication errors during token refresh**: if the SDK decides when to refresh a token based on the local clock, a lagging clock may cause the client to send an already expired token to the cluster.

Such errors often occur only on some hosts — for example, on new machines or in environments without configured time synchronization.

## Diagnostics {#diagnostics}

### Using chrony

[chrony](https://chrony-project.org/documentation) is an NTP client.

To check time synchronization on the client host, run:

```bash
chronyc tracking
chronyc sources -v
```

In `chronyc tracking`, check:

- `System time` — the current offset of the system clock relative to the NTP clock of `chrony` (in seconds).
- `Leap status` — the [leap second](*leap-second) and clock synchronization status. In the normal state the value is `Normal` (no warning). The `Not synchronised` value means `chrony` does not consider the clock synchronized.
- `Reference ID` — the ID and name (or IP address) of the synchronization source. If `Reference ID = 7F7F0101` and no name or IP address is shown, `chrony` is in local mode without external synchronization.

{% cut "Example chronyc tracking output" %}

```text
Reference ID    : CB00710F (ntp1.example.net)
Stratum         : 3
Ref time (UTC)  : Fri Jan 23 09:49:17 2026
System time     : 0.000006523 seconds slow of NTP time
Last offset     : -0.000006747 seconds
RMS offset      : 0.000035822 seconds
Frequency       : 3.225 ppm slow
Residual freq   : -0.000 ppm
Skew            : 0.129 ppm
Root delay      : 0.013639022 seconds
Root dispersion : 0.001100737 seconds
Update interval : 64.2 seconds
Leap status     : Normal
```

{% endcut %}

In `chronyc sources -v`, find the source marked `*` in column `S`: it is selected for synchronization. Verify that it is the expected time source: compare it with the source in the `chrony` configuration on that host.

{% cut "Example chronyc sources -v output" %}

```text
MS Name/IP address         Stratum Poll Reach LastRx Last sample
===============================================================================
#* GPS0                          0   4   377    11   -479ns[ -621ns] +/-  134ns
^? ntp1.example.net              2   6   377    23   -923us[ -924us] +/-   43ms
^+ ntp2.example.net              1   6   377    21  -2629us[-2619us] +/-   86ms
```

{% endcut %}

### Using timedatectl

[timedatectl](https://www.freedesktop.org/software/systemd/man/latest/timedatectl.html) is a systemd utility for system clock status.

On hosts with systemd, run:

```bash
timedatectl status
```

In the output, check these lines and their values:

- `System clock synchronized: yes`
- `NTP service: active`

{% cut "Example timedatectl status output" %}

```text
               Local time: Thu 2026-09-17 16:08:56 CEST
           Universal time: Thu 2026-09-17 14:08:56 UTC
                 RTC time: Thu 2026-09-17 14:08:56
                Time zone: Europe/Warsaw (CEST, +0200)
System clock synchronized: yes
              NTP service: active
          RTC in local TZ: no
```

{% endcut %}

{% note info %}

If neither `chrony` nor `timedatectl` is available, check time synchronization with the NTP client or platform tools used in your environment. For example, if `ntpd` is installed:

```bash
ntpq -p
```

The command lists the NTP servers the client is connected to and their status. The source marked `*` is the current synchronization source (system peer).

{% endnote %}

## Recommendations {#recommendations}

**The primary fix is to synchronize the system clocks.**

Configure time synchronization on client hosts with `chrony` or `ntpd` and multiple NTP sources from your environment. For configuration examples, see [{#T}](performance/system/system-clock-drift.md#ntp-examples).

{% include [example_clock_skew_internal](_includes/example_clock_skew_internal.md) %}

## See also

- [{#T}](performance/system/system-clock-drift.md)

[*leap-second]: A one-second adjustment of UTC by ±1 second to keep the timescale aligned with Earth’s rotation.
