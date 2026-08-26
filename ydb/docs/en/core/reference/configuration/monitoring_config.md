# monitoring_config

The `monitoring_config` section of the {{ ydb-short-name }} configuration file configures [YDB Monitoring](../ydb-ui/ydb-monitoring.md).

## Authentication on monitoring pages {#authentication}

This section describes settings related to [authentication](../../security/authentication.md) on individual embedded monitoring pages.

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

Example of enabling authentication on individual embedded monitoring pages.

```yaml
monitoring_config:
  # authentication on the /counters and /healthcheck pages
  require_counters_authentication: true
  require_healthcheck_authentication: true
```

## TLS on monitoring pages {#tls}

{{ ydb-short-name }} exposes a separate HTTP network port for running the [Embedded UI](../../reference/ydb-ui/index.md), exposing [metrics](../../devops/observability/monitoring.md), and other miscellaneous endpoints.

You can enable [TLS](https://en.wikipedia.org/wiki/Transport_Layer_Security) on the HTTP port so that it accepts only HTTPS connections. Plain HTTP requests are rejected at the TLS handshake without a response and without redirect to HTTPS. Simultaneous HTTP and HTTPS on different ports is not supported: monitoring uses a single port that works either as HTTP or as HTTPS.

TLS is enabled by specifying an SSL certificate (certificate chains are supported) and a private SSL key (RSA, ECDSA, and PKCS#8 keys are supported) without a password, because encrypted keys are not supported.

The TLS parameters for monitoring are described below. When you change these parameters, restart the cluster nodes where the changes were made.

#|
|| Parameter | Description ||
||

`monitoring_certificate`

|

Parameter for passing the SSL certificate and private SSL key contents directly in [PEM format](https://en.wikipedia.org/wiki/Privacy-Enhanced_Mail) without separate files. Requirements for the parameter value:

- the server certificate is specified first;

- intermediate certificates are listed next, if any;

- a passwordless private key is specified last.

When this parameter is set, the embedded UI automatically handles requests using the specified SSL certificate. If `monitoring_certificate` is set, the `monitoring_certificate_file` and `monitoring_private_key_file` parameters are ignored.

Default value: an empty string.

||
||

`monitoring_certificate_file`

|

Path to the SSL certificate file in PEM format. The file may also contain a passwordless private SSL key. That private key is used if `monitoring_private_key_file` is not set.

When `monitoring_certificate_file` is set, the embedded UI automatically handles requests using the specified SSL certificate.

Default value: an empty string.

||
||

`monitoring_private_key_file`

|

Path to the private SSL key file in PEM format without a password. When this parameter is set, `monitoring_certificate_file` must also be set. If the file specified in `monitoring_certificate_file` contains a private SSL key, it is ignored; `monitoring_private_key_file` takes priority for the private key.

Default value: an empty string.

||
||

`monitoring_ca_file`

|

Path to the root (CA) certificate file. Enables client certificate request during the TLS handshake for monitoring.

Valid values:

- A non-empty path: the server requests a client certificate. If a certificate is presented, it is verified at the TLS level during [device authentication](../../security/authentication.md#device-auth-interfaces) (a connection with an untrusted certificate is not established); after successful verification, the server preferentially uses the [authentication token](../../concepts/glossary.md#auth-token) to authenticate the client, and if no token is present, authentication is performed using the [client certificate](../../security/authentication.md#client-certificate). If no certificate is presented, then with the `client_certificate_required: true` setting, the connection is not established.

- An empty string: the server does not request a client certificate during the TLS handshake; device authentication and client certificate authentication for monitoring are unavailable with this setting.

The parameter is ignored if TLS is not enabled via `monitoring_certificate` or `monitoring_certificate_file` + `monitoring_private_key_file`.

Default value: an empty string.

||
||

`client_certificate_required`

|

Requirement for a client certificate during the TLS handshake for monitoring.

Valid values:

- `true`: the server requires a client certificate: a connection without a certificate or with an untrusted certificate is not established. `true` can only be specified together with a non-empty `monitoring_ca_file`.

- `false`: a client certificate is not required; whether a certificate is requested is determined by `monitoring_ca_file`.

Default value: `false`.

||
|#

Example of enabling mTLS for monitoring: request a client certificate without requiring its presentation.

```yaml
monitoring_config:
  monitoring_certificate_file: /path/to/cert.pem # enable TLS
  monitoring_private_key_file: /path/to/key.pem
  monitoring_ca_file: /path/to/ca.pem # request a client certificate (mTLS)
```

Example of enabling mTLS for monitoring: require a client certificate with mandatory presentation.

```yaml
monitoring_config:
  monitoring_certificate_file: /path/to/cert.pem
  monitoring_private_key_file: /path/to/key.pem
  monitoring_ca_file: /path/to/ca.pem
  client_certificate_required: true
```
