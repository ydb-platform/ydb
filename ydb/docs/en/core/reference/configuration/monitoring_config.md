# monitoring_config

The `monitoring_config` section of the {{ ydb-short-name }} configuration file defines parameters for [YDB Monitoring](../embedded-ui/ydb-monitoring.md). Below are the settings related to [authentication](../../security/authentication.md) on specific pages of the built-in monitoring.

```yaml
monitoring_config:
  # authentication on /counters and /healthcheck pages
  require_counters_authentication: false
  require_healthcheck_authentication: false
  # list of paths for which authentication is disabled
  disabled_authentication_paths:
    - /ver
    - /trace
```

## Authentication on Monitoring Pages {#authentication}

#|
|| Parameter | Description ||
|| `require_counters_authentication` | Mandatory [authentication](../../security/authentication.md) mode on `/counters` and `/counters/hosts` pages.

Possible values:

- `true` — access to `/counters` and `/counters/hosts` is allowed only with an [authentication token](../../concepts/glossary.md#auth-token); requests undergo authentication and permission checks.

    The value `true` is only valid when mandatory [authentication](../../security/authentication.md) is enabled in the [security_config](./security_config.md) section of the {{ ydb-short-name }} configuration file.

- `false` — requests to `/counters` and `/counters/hosts` can be made without an [authentication token](../../concepts/glossary.md#auth-token).

Default value: `false`.
    ||
|| `require_healthcheck_authentication` | Additional [authentication](../../security/authentication.md) requirement for the `/healthcheck` endpoint on top of general cluster rules.

Possible values:

- `true` — any `/healthcheck` response, including the [Prometheus format](https://prometheus.io/docs/instrumenting/exposition_formats/) (the `format=prometheus` parameter), is returned only when requested with an [authentication token](../../concepts/glossary.md#auth-token); requests undergo authentication and permission checks.

    The value `true` is only valid when mandatory [authentication](../../security/authentication.md) is enabled in the [security_config](./security_config.md) section of the {{ ydb-short-name }} configuration file.

- `false` — when mandatory authentication is enabled in the cluster, requests to `/healthcheck` without a token are still allowed if output in the [Prometheus format](https://prometheus.io/docs/instrumenting/exposition_formats/) (`format=prometheus`) is requested. For other `/healthcheck` response formats, general rules apply (see the note below).

Default value: `false`.

{% note info %}

If mandatory [authentication](../../security/authentication.md) is enabled in [security_config](./security_config.md), a token is required for `/healthcheck` responses in any format other than Prometheus, regardless of the `require_healthcheck_authentication` value.

{% endnote %}

||
|| `disabled_authentication_paths` | A list of built-in monitoring paths for which mandatory [authentication](../../security/authentication.md) is disabled (even if the mandatory authentication mode `enforce_user_token_requirement` is enabled in [security_config](./security_config.md)).

The following rules apply when matching paths:
- The path must match exactly (for example, `/ver` or `/trace`).
- Query parameters are stripped before matching (for example, a `/ver?foo=bar` request will successfully match `/ver`).
- Subpaths are not matched automatically (for example, if authentication is disabled for `/ver`, authentication is still required for `/ver/subpath`).
- Prefixes are not matched automatically (for example, `/prefix/ver` will still require authentication).

Default value: empty list.
||
|#
