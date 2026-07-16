# monitoring_config

The `monitoring_config` section of the {{ ydb-short-name }} configuration file configures [YDB Monitoring](../embedded-ui/ydb-monitoring.md). The parameters below control [authentication](../../security/authentication.md) on individual embedded monitoring pages.

```yaml
monitoring_config:
  # authentication on the /counters and /healthcheck pages
  require_counters_authentication: false
  require_healthcheck_authentication: false
```

## Authentication on Monitoring Pages {#authentication}

#|
|| Parameter | Description ||
|| `require_counters_authentication` | Selects mandatory [authentication](../../security/authentication.md) mode for the `/counters` and `/counters/hosts` pages.

Valid values:

- `true`: Access to `/counters` and `/counters/hosts` requires an [auth token](../../concepts/glossary.md#auth-token). Requests undergo authentication and authorization.

    The `true` value is allowed only when mandatory [authentication](../../security/authentication.md) is enabled in the [security_config](./security_config.md) section of the {{ ydb-short-name }} configuration file.

- `false`: Requests to `/counters` and `/counters/hosts` can be made without an [auth token](../../concepts/glossary.md#auth-token).

Default value: `false`.
    ||
|| `require_healthcheck_authentication` | Adds an [authentication](../../security/authentication.md) requirement for the `/healthcheck` endpoint on top of the cluster-wide rules.

Valid values:

- `true`: Any `/healthcheck` response, including [Prometheus format](https://prometheus.io/docs/instrumenting/exposition_formats/) output (the `format=prometheus` parameter), is returned only for requests with an [auth token](../../concepts/glossary.md#auth-token). Requests undergo authentication and authorization.

    The `true` value is allowed only when mandatory [authentication](../../security/authentication.md) is enabled in the [security_config](./security_config.md) section of the {{ ydb-short-name }} configuration file.

- `false`: When mandatory authentication is enabled in the cluster, requests to `/healthcheck` without a token are still allowed if [Prometheus format](https://prometheus.io/docs/instrumenting/exposition_formats/) output is requested (`format=prometheus`). Cluster-wide rules apply to all other `/healthcheck` response formats (see the note below).

Default value: `false`.

{% note info %}

If mandatory [authentication](../../security/authentication.md) is enabled in [security_config](./security_config.md), an auth token is required for `/healthcheck` responses in any format other than Prometheus, regardless of the `require_healthcheck_authentication` value.

{% endnote %}

||
|#
