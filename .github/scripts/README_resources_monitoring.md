# Resource monitoring during ya make

During `ya make` execution, the test_ya action collects system metrics every 3 seconds:

- **CPU**: total % and per-process (top 50 PIDs by CPU)
- **RAM**: used memory (MemTotal - MemAvailable)
- **Disk I/O**: read/write sectors (delta MB per sample)

## Output files

| File | Description |
|------|-------------|
| `resources_monitor.jsonl` | Raw samples, one JSON object per line |
| `resources_report.html` | Interactive Plotly chart with CPU/RAM/disk + test intervals overlay |
| `resources_trace.json` | Chromium trace format (counter events) for chrome://tracing or Perfetto |
| `ram_usage.txt` | Legacy format for report_ram_analyzer (ts used_kb) |

## JSONL format (resources_monitor.jsonl)

Each record has **absolute** (system-wide) and **ya make** (process tree) metrics:

```json
{
  "ts": 1772558713.907,
  "ts_us": 1772558713907444,
  "cpu_total_pct": 45.2,
  "cpu_ya_pct": 38.1,
  "cpu_per_pid": [{"pid": 1234, "comm": "clang++", "cpu_pct": 12.5, "utime": 100, "stime": 20}],
  "ram_used_kb": 16000000,
  "ram_ya_kb": 12000000,
  "disk_read_mb_delta": 2.5,
  "disk_write_mb_delta": 0.1,
  "disk_ya_read_mb_delta": 2.1,
  "disk_ya_write_mb_delta": 0.08
}
```

- `cpu_per_pid`: **all** processes in ya make tree (no top-N limit)
- `*_ya_*`: sum over ya make process tree (ya, ya-tc, clang++, tests, etc.)

## Correlation with report.json

- `report.json` has `suite_start_timestamp` and `wall_time` for each test chunk
- Resource samples use Unix timestamp (`ts`)
- The analyzer overlays test intervals (green/red bands) on the resource charts
- For trace alignment: `ts_us` matches Chromium trace microseconds

## Trace merge

To view resources alongside ya timeline in chrome://tracing or Perfetto:

1. Load `resources_trace.json` (counter events for CPU, RAM, disk)
2. Load the ya timeline trace (from `ya analyze-make timeline`)
3. Both use the same trace format; Perfetto merges multiple files

## Customization

- `--interval 3`: sampling interval (1–5 sec recommended)
