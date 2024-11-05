# Enabling/disabling Scrubbing

Scrubbing is a process that reads data, checks its integrity, and restores it if needed. The process is run by default. The interval between completing a scrub and starting the next one is 1 month. You can change the interval using [{{ ydb-short-name }} DSTool](../../reference/ydb-dstool/index.md). The process checks data that was accessed before the previous scrub. Scrubbing is started and stopped for the entire {{ ydb-short-name }} cluster. Scrubbing is performed in the background without overloading the system.

To set a 48-hour interval, run the command:

```bash
ydb-dstool -e <bs_endpoint> cluster set --scrub-periodicity 48h
```

You can also set the maximum number of cluster disks to be scrubbed at a time. For example, to only scrub one disk at a time, run the command:

```bash
ydb-dstool -e <bs_endpoint> cluster set --max-scrubbed-disks-at-once
```

To stop cluster scrubbing, run the command:

```bash
ydb-dstool -e <bs_endpoint> cluster set --scrub-periodicity disable
```
