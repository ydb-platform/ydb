# Charts

To view charts, use Grafana.

The main metrics of the system are displayed on the dashboard:

* **CPU Usage**: The total CPU utilization on all nodes (1&nbsp;000&nbsp;000 = 1 CPU).
* **Memory Usage**: RAM utilization by nodes.
* **Disk Space Usage**: Disk space utilization by nodes.
* **SelfPing**: The highest actual delivery time of deferred messages in the actor system over the measurement interval. Measured for messages with a 10 ms delivery delay. If this value grows, it might indicate microbursts of workload, high CPU utilization, or displacement of the {{ ydb-short-name }} process from CPU cores by other processes.

