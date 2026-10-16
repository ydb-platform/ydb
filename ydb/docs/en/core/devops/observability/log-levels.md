# Optimal logging level for production

In {{ ydb-short-name }}, log verbosity is configured per component. In production you normally record significant events only; during an incident you temporarily increase the level and revert after collecting data.

For the full list of levels and `log_config` parameters, see [{#T}](../../reference/configuration/log_config.md). To change levels in the UI, see [{#T}](../../reference/ydb-ui/logs.md#change_log_level).

## Log levels {#levels}

{{ ydb-short-name }} defines **nine** log levels. In configuration (`default_level`, the `level` field in `entry`) they are set as **integers from 0 to 8**: a **lower** number means a **more critical** event (write priority); a **higher** number means a **more verbose** routine stream. The value `5` (`NOTICE`) is the typical cluster default.

| Level | Value | Purpose |
| --- | --- | --- |
| `EMERG` | 0 | Possible system failure (for example, cluster outage) |
| `ALERT` | 1 | Possible system degradation |
| `CRIT` | 2 | Critical condition |
| `ERROR` | 3 | Non-critical error |
| `WARN` | 4 | Warning that requires attention |
| `NOTICE` | 5 | Significant events |
| `INFO` | 6 | Context for statistics and routine monitoring |
| `DEBUG` | 7 | Details for troubleshooting |
| `TRACE` | 8 | Maximum verbosity |

Full wording and `log_config` parameters are in [{#T}](../../reference/configuration/log_config.md#log-levels).

**The rest of this article** covers **production** verbosity: baseline levels and when to temporarily raise to `INFO`, `DEBUG`, or `TRACE`. Levels `EMERG` through `WARN` reflect errors and emergencies—you do not select them manually as a “logging mode”; components emit them when the corresponding condition occurs.

## Which level to use when {#when}

Rule of thumb:

- **`INFO`** — normal cluster operation when you need slightly more context than `NOTICE` without a full debug stream.
- **`DEBUG`** — diagnosing unstable behavior, incidents, reproducing issues on a staging cluster or a limited set of components.
- **`TRACE`** — deep investigation of isolated cases; enable **selectively** (component, node, limited time window), not cluster-wide without a strong reason.

In production, keep the baseline at **`NOTICE`** (or `INFO` if your observability policy requires it). Do not leave `DEBUG` or `TRACE` enabled globally—log volume and write load increase.

## How to change the level {#how}

### Static configuration

Set the `log_config` section in the cluster configuration, for example:

```yaml
log_config:
  default_level: 5  # NOTICE
  entry:
    - component: "KQP"
      level: 6  # INFO
```

See [{#T}](../../reference/configuration/log_config.md) for `default_level`, `entry`, sampling, and output format.

### Without restarting nodes (dynamic configuration)

The `log_config` section is [dynamically updatable](../configuration-management/configuration-v1/dynamic-config.md#dynamic-kinds). Update the cluster YAML and apply it, for example:

```bash
{{ ydb-cli }} admin config replace -f dynconfig.yaml
```

After the configuration propagates, new levels take effect without restarting nodes.

### Via {{ ydb-ui-name }} (CMS)

If the cluster still uses CMS-based management, change levels under **Configs** → `LogConfigItems` as described in [{#T}](../../reference/ydb-ui/logs.md#change_log_level).

## When to raise the level {#raise}

Increase verbosity if:

- logs **lack context** to explain a failure or degradation;
- you see **unstable behavior** (intermittent errors, timeouts, flaky symptoms);
- you need to **reproduce** an issue and capture the event sequence.

Raise in steps: start with `INFO` or `DEBUG` for a **specific component**, then use `TRACE` only if needed and for a limited time. Example (for transaction lock invalidation, see [{#T}](../../troubleshooting/performance/queries/tli-logging.md)):

```yaml
log_config:
  default_level: 5
  entry:
    - component: "TLI"
      level: 6  # INFO
```

## When to lower the level {#lower}

After troubleshooting, **restore** the previous levels when:

- logs for analysis are **collected**;
- the issue is **reproduced** and handed off, or the incident is **closed**;
- the temporary increase is **no longer needed**.

Otherwise log volume, disk and CPU load stay high, and new events are harder to spot.

## Limits and risks {#limits}

Higher verbosity means more log lines:

- **Volume** — disks and journals (`journald`, files, external collectors) fill faster.
- **CPU** — formatting and writing log records.
- **Noise** — on `DEBUG` and `TRACE`, rare errors are harder to find.

To reduce overload, use `log_config` options:

- `allow_drop_entries` and `time_threshold_ms` — buffering and dropping lower-priority records when the log pipeline is saturated;
- `default_sampling_level`, `default_sampling_rate`, and `sampling_*` in `entry` — partial recording between the main and sampling levels (see [{#T}](../../reference/configuration/log_config.md)).

Request tracing (OpenTelemetry) is configured **separately** from log levels—see [{#T}](../../reference/observability/tracing/setup.md). The **`fraction`** field belongs to **`tracing_config.sampling`**, not `log_config`: it sets the share of traced requests, not log lines.

```yaml
tracing_config:
  sampling:
    - level: 10  # trace detail; see the reference
      fraction: 0.01  # roughly 1% of requests
    - level: 14
      fraction: 0.001  # deeper detail — even less often
```

### Example production configuration

```yaml
log_config:
  default_level: 5  # NOTICE — baseline for production
  default_sampling_level: 7  # DEBUG — sampling band
  default_sampling_rate: 10  # roughly every tenth message between NOTICE and DEBUG

  entry:
    - component: "KQP"
      level: 5  # NOTICE
      sampling_level: 7  # DEBUG
      sampling_rate: 50  # roughly every 50th DEBUG message for KQP
    - component: "BLOBSTORAGE"
      level: 5
      sampling_level: 8  # TRACE
      sampling_rate: 100

  # Lower load when the log pipeline is saturated
  allow_drop_entries: true  # allow dropping lower-priority records
  time_threshold_ms: 1000   # buffer flush period, ms

  # Output format
  format: "json"  # easier to parse
  cluster_name: "production-cluster"  # cluster name in json output
```

### Example of excessive verbosity

```yaml
log_config:
  default_level: 8  # TRACE for the whole cluster
  default_sampling_level: 8
  default_sampling_rate: 1

  entry:
    - component: "*"
      level: 8
      sampling_level: 8
      sampling_rate: 1
```

This produces a very large log stream, increases system load, and makes analysis harder. Use it only briefly on staging, not as production baseline.
