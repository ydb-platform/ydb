# Setting up monitoring for a local {{ ydb-short-name }} cluster

This page provides instructions on how to set up monitoring for a local {{ ydb-short-name }} cluster that is deployed using [Quick start](../../quickstart.md).

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

1. [Install](https://prometheus.io/docs/prometheus/latest/getting_started) Prometheus.

1. Edit the Prometheus [configuration file](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/grafana_dashboards/local_ydb_prometheus.yml):

    1. In the `targets` section specify addresses of all servers of the {{ ydb-short-name }} cluster and ports for each storage and database node that runs on the server.

        For example, for the {{ ydb-short-name }} cluster that contains three servers, each server running one storage node on port 8765 and two database nodes on ports 8766 and 8767, specify nine addresses for all metrics subgroups except for the disk subgroups (for disk metrics subgroups, specify only storage node addresses):

        ```json
        static_configs:
        - targets:
          - ydb-s1.example.com:8765
          - ydb-s1.example.com:8766
          - ydb-s1.example.com:8767
          - ydb-s2.example.com:8765
          - ydb-s2.example.com:8766
          - ydb-s2.example.com:8767
          - ydb-s3.example.com:8765
          - ydb-s3.example.com:8766
          - ydb-s3.example.com:8767
        ```

        For a local single-node {{ ydb-short-name }} cluster, specify one address in the `targets` section:

        ```json
        - targets: ["localhost:8765"]
        ```

    2. If necessary, in the `tls_config` section, specify the [CA-issued certificate](../manual/initial-deployment.md#tls-certificates) used to sign the other TLS certificates of the {{ ydb-short-name }} cluster:

       ```json
       tls_config:
           ca_file: '<ydb-ca-file>'
       ```

2. [Run](https://prometheus.io/docs/prometheus/latest/getting_started/#starting-prometheus) Prometheus using the edited configuration file.

3. [Install and start](https://grafana.com/docs/grafana/latest/getting-started/getting-started/) Grafana.

4. [Create](https://prometheus.io/docs/visualization/grafana/#creating-a-prometheus-data-source) a data source of the `prometheus` type in Grafana, and attach it to the running Prometheus instance.

5. Upload [{{ ydb-short-name }} dashboards](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/helm/ydb-prometheus/dashboards) to Grafana.

To upload dashboards, use the Grafana UI [Import](https://grafana.com/docs/grafana/latest/dashboards/export-import/#import-dashboard) tool or run a [script](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/grafana_dashboards/local_upload_dashboards.sh). Please note that the script uses [basic authentication](https://grafana.com/docs/grafana/latest/http_api/create-api-tokens-for-org/#authentication) in Grafana. For other cases, modify the script.

Review the dashboard [metric reference](../../reference/observability/metrics/grafana-dashboards.md).
