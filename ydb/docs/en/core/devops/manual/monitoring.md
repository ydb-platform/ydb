# Setting up monitoring for a local {{ ydb-short-name }} cluster

This page provides instructions on how to set up monitoring for a local YDB cluster that is deployed using [Quick start](../../quickstart.md).

{{ ydb-short-name }} has multiple system health sensors. Instant sensor values are available in the web interface:

```http
http://localhost:31002/counters/
```

Linked sensors are grouped into a subgroup (such as `counters auth`). To only view sensor values for a particular subgroup, follow a URL like:

```http
http://localhost:31002/counters/counters=<servicename>/
```

* `<servicename>`: Sensor subgroup name.

> For example, data about the utilization of server hardware resources is available at the URL:
>
> ```http
> http://localhost:31002/counters/counters=utils
> ```

You can collect metric values using [Prometheus](https://prometheus.io/), a popular open-source tool. {{ ydb-short-name }} sensor values in [Prometheus format](https://prometheus.io/docs/instrumenting/exposition_formats/) are available at a URL in the following format:

```http
http://localhost:31002/counters/counters=<servicename>/prometheus
```

* `<servicename>`: Sensor subgroup name.

To visualize data, use any system that supports Prometheus, such as [Zabbix](https://www.zabbix.com/), [Amazon CloudWatch](https://aws.amazon.com/cloudwatch/), or [Grafana](https://grafana.com/):

![grafana-actors](../../_assets/grafana-actors.png)

## Setting up monitoring with Prometheus and Grafana {#prometheus-grafana}

To set up monitoring for a local single-node {{ ydb-short-name }} cluster using [Prometheus](https://prometheus.io/) and [Grafana](https://grafana.com/):

1. [Install and run](https://prometheus.io/docs/prometheus/latest/getting_started/#downloading-and-running-prometheus) Prometheus via a [configuration file](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/grafana_dashboards/local_ydb_prometheus.yml).
1. [Install and start](https://grafana.com/docs/grafana/latest/getting-started/getting-started/) the Grafana.
1. [Create](https://prometheus.io/docs/visualization/grafana/#creating-a-prometheus-data-source) a data source of the `prometheus` type in Grafana and attach it to a running Prometheus instance.
1. Upload [{{ ydb-short-name }} dashboards](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/grafana_dashboards/) to Grafana.

   To upload dashboards, use the Grafana UI [Import](https://grafana.com/docs/grafana/latest/dashboards/export-import/#import-dashboard) tool or run a [script](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/grafana_dashboards/local_upload_dashboards.sh). Please note that the script uses [basic authentication](https://grafana.com/docs/grafana/latest/http_api/create-api-tokens-for-org/#authentication) in Grafana. For other cases, modify the script.

   Review the dashboard [metric reference](../../reference/observability/metrics/grafana-dashboards.md).
