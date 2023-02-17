# YDB Monitoring Prometheus

Chart with resources for monitoring YDB clusters with Prometheus and Grafana.

This chart depends on [kube-prometheus-stack](https://github.com/prometheus-community/helm-charts/tree/8a4f9ea1fb0fe32f3169cbfbd9f0fc517f4aaa10/charts/kube-prometheus-stack).

This chart installs following resources:

* Prometheus [additional scrape configs](https://github.com/prometheus-community/helm-charts/blob/8a4f9ea1fb0fe32f3169cbfbd9f0fc517f4aaa10/charts/kube-prometheus-stack/values.yaml#L3031) for external cluster
* Prometheus service-monitor object for internal cluster
* Configmaps with Grafana dashboards

## Monitoring External YDB Cluster (bare metal or virtual machines)

1. Set following in values.yaml:

```yaml
kube-prometheus-stack:
  prometheus:
    prometheusSpec:
      additionalScrapeConfigsSecret:
        enabled: true
        name: ydb-prometheus-additional-scrape-configs
        key: additional-scrape-configs.yaml
```

2. Secret (ydb-prometheus-additional-scrape-configs) will be generated with chart installation and referenced in prometheus CRD.

3. Set following in values.yaml to cluster monitor:

```yaml

ydb:
  clusters:
  - name: <cluster-name>
    type: external
    storage:
      name: <prometheus job name, will be formatted as ydb/<name>/counter/<counter name>>
      hosts:
      - <ydb storage hosts>
      port:
        number: <ydb storage monitoring port number>
    databases:
    - name: <prometheus job name, will be formatted as ydb/<name>/counter/<counter name>>
      hosts:
      - <ydb dynamic hosts>
      port:
        number: <ydb dynamic monitoring port number>

```

4. Install chart with `helm`

## Monitoring Internal YDB Cluster (deployed with ydb-operator)

1. Set following in values.yaml to cluster monitor:

```yaml

ydb:
  clusters:
  - name: <cluster-name>
    type: internal
    storage:
      name: <prometheus job name, will be formatted as serviceMonitor/<release namespace>/<release-name>/<counter number>
      namespace: <ydb cluster namespace>
      selector: <kubernetes label selector for ydb storage pods>
      port:
        name: <ydb storage service (status) port number>
    databases:
    - name: <prometheus job name, will be formatted as serviceMonitor/<release namespace>/<release-name>/<counter number>
      namespace: <ydb cluster namespace>
      selector: <kubernetes label selector for ydb database pods>
      port:
        name: <ydb database service (status) port number>

```

2. Install chart with `helm`
