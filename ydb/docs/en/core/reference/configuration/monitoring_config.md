# monitoring_config

The `monitoring_config` section of the {{ ydb-short-name }} configuration file sets parameters for [YDB Monitoring](../embedded-ui/ydb-monitoring.md). Below are the settings related to [authentication](../../security/authentication.md) on individual pages of the built-in monitoring.


```yaml
monitoring_config:
  # authentication on pages /counters and /healthcheck
  require_counters_authentication: false
  require_healthcheck_authentication: false
```


## Authentication on monitoring pages {#authentication}

#|
|| Parameter | Description ||
|| `require_counters_authentication` | Mandatory [authentication](../../security/authentication.md) mode on the `/counters` and `/counters/hosts` pages.

Possible values:

- `true` — access to `/counters` and `/counters/hosts` only with an [authentication token](../../concepts/glossary.md#auth-token); requests undergo authentication and permission verification.

  The `true` value is only allowed when mandatory [authentication](../../security/authentication.md) mode is enabled in the [security_config](./security_config.md) section of the {{ ydb-short-name }} configuration file.
- `false` — requests to `/counters` and `/counters/hosts` can be made without an [authentication token](../../concepts/glossary.md#auth-token).

Default value: `false`.
||
|| `require_healthcheck_authentication` | Additional [authentication](../../security/authentication.md) requirement for the `/healthcheck` endpoint on top of the cluster's general rules.

Possible values:

- `true` — any `/healthcheck` response, including the [Prometheus format](https://prometheus.io/docs/instrumenting/exposition_formats/) (the `format=prometheus` parameter), is only returned when requested with an [authentication token](../../concepts/glossary.md#auth-token); requests undergo authentication and permission verification.

  The `true` value is only allowed when mandatory [authentication](../../security/authentication.md) mode is enabled in the [security_config](./security_config.md) section of the {{ ydb-short-name }} configuration file.
- `false` — when mandatory authentication is enabled in the cluster, requests to `/healthcheck` without a token are still allowed if the output is requested in the [Prometheus format](https://prometheus.io/docs/instrumenting/exposition_formats/) (`format=prometheus`). For other response formats `/healthcheck`, the general rules apply (see note below).

Default value: `false`.

{% note info %}

If mandatory [authentication](../../security/authentication.md) is enabled in [security_config](./security_config.md), then for `/healthcheck` responses in any format other than Prometheus, a token is required regardless of the `require_healthcheck_authentication` value.

{% endnote %}

||
|#
