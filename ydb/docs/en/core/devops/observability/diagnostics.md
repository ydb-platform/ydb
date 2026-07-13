# Diagnostics of an isolated cluster

If a cluster is hidden behind network barriers, direct monitoring of its state is impossible. In such cases, to identify problems in the cluster's internal environment, you can use the mechanism of saving its state to a file.

## Collecting diagnostic information

To collect diagnostic information, use the command:


```bash
ydb [global options...] admin cluster diagnostics collect \
  --duration 200 \
  --period 15 \
  --output diagnostics.tar
```


### Command parameters

* `--duration` — the number of seconds during which information about the cluster will be collected.
* `--period` — the interval in seconds between metric collections.
* `--output` — the path and file name in `.tar` format, where all collected cluster information will be written.
* `--no-sanitize` — enables a full report by disabling the cleanup of user table names, column names, and queries (which are stripped by default).

The command writes the internal state of cluster nodes to the specified file over a given period. Every `period` seconds, metrics are collected from each cluster node and written to the file.

## Archive contents

The resulting file is an archive and contains:

* Metrics collected from cluster nodes.
* Cluster information.

All data is written in JSON format in a human-readable form.

### Cluster information

Cluster information includes:

* Health Check report.
* Queries to the cluster partition tables `/Root/.sys` and their responses.
* Current configuration of cluster subsystems.
* Current state of cluster nodes.
