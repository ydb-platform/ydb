# monitoring_config

The `monitoring_config` section of the {{ ydb-short-name }} configuration file sets the parameters for YDB Monitoring.

## Authentication on monitoring pages {#authentication}

This section describes the settings related to [authentication](../../security/authentication.md) on individual pages of the built-in monitoring.

#|
|| Parameter | Description ||
|| `require_counters_authentication` | Mandatory authentication mode on the `/counters` and `/counters/hosts` pages.

Possible values:

- `true` — access to `/counters` and `/counters/hosts` is only allowed with an [authentication token](../../concepts/glossary.md#auth-token); requests undergo authentication and [permission](../../concepts/glossary.md#access-right) checks.

  The `true` value is only allowed when mandatory authentication is enabled in the [security_config](./security_config.md) section of the {{ ydb-short-name }} configuration file.
- `false` — requests to `/counters` and `/counters/hosts` can be made without an authentication token.

Default value: `false`.
||
|| `require_healthcheck_authentication` | Additional authentication requirement for the `/healthcheck` endpoint on top of the cluster-wide rules.

Possible values:

- `true` — any `/healthcheck` response, including the [Prometheus format](https://prometheus.io/docs/instrumenting/exposition_formats/) (the `format=prometheus` parameter), is only returned when the request includes an authentication token; requests undergo authentication and permission checks.

  The `true` value is only allowed when mandatory authentication is enabled in the security_config section of the {{ ydb-short-name }} configuration file.
- `false` — when mandatory authentication is enabled in the cluster, requests to `/healthcheck` without a token are still allowed if the requested output is in the Prometheus format (`format=prometheus`). For other response formats, `/healthcheck` follows the general rules (see the note below).

Default value: `false`.

{% note info %}

If mandatory authentication is enabled in security_config, then for `/healthcheck` responses in any format other than Prometheus, a token is required regardless of the `require_healthcheck_authentication` value.

{% endnote %}

||
|#

Example of setting parameters with authentication enabled on individual pages of the built-in monitoring.


```yaml
monitoring_config:
  # authentication on /counters and /healthcheck pages
  require_counters_authentication: true
  require_healthcheck_authentication: true
```


## TLS on monitoring pages {#tls}

{{ ydb-short-name }} opens a separate HTTP port for the built-in interface, displaying [metrics](../../devops/observability/monitoring.md), and other auxiliary commands.

You can enable [TLS](https://en.wikipedia.org/wiki/Transport_Layer_Security) on the HTTP port, so the port will only accept HTTPS connections. Regular HTTP requests will then be rejected at the TLS handshake level without any response and without a redirect to HTTPS. Running HTTP and HTTPS simultaneously on different ports is not supported: monitoring uses a single port that works either as HTTP or as HTTPS only.

TLS is enabled by specifying an SSL certificate (certificate chains are supported) and a private SSL key (keys of the following types are supported: RSA, ECDSA, and PKCS#8) without a password, as encrypted keys are not supported.

The TLS parameters for monitoring are described below. When changing the values of these parameters, you must restart the cluster nodes on which the changes were made.

#|
|| Parameter | Description ||
||

`monitoring_certificate`

|

Parameter for passing the contents of the SSL certificate and private SSL key directly in [PEM format](https://en.wikipedia.org/wiki/Privacy-Enhanced_Mail) without using separate files. Requirements for the parameter contents:

- the server certificate is specified first;
- then intermediate certificates are specified sequentially, if any;
- then the private key without a password is specified.

When this parameter is specified, the built-in interface automatically starts processing requests using the specified SSL certificate. If the `monitoring_certificate` parameter is specified, the `monitoring_certificate_file` and `monitoring_private_key_file` parameters are ignored.

Default value: empty string.

||
||

`monitoring_certificate_file`

|

Path to the certificate file for SSL access in PEM format. The file may additionally contain a private SSL key without a password. This private key will be used if the `monitoring_private_key_file` parameter is not specified.

When the `monitoring_certificate_file` parameter is specified, the built-in interface automatically starts processing requests using the specified SSL certificate.

Default value: empty string.

||
||

`monitoring_private_key_file`
|

Path to the private SSL key file in PEM format without a password. When this parameter is specified, the `monitoring_certificate_file` parameter must be set. If the file specified in the `monitoring_certificate_file` parameter contains a private SSL key, it will be ignored, meaning the `monitoring_private_key_file` parameter takes priority in specifying the private key.

Default value: empty string.

||
||

`monitoring_ca_file`

|

Path to the root (CA) certificate file. Enables requesting a client certificate during the TLS handshake for monitoring.

Possible values:

- Non-empty path — the server requests a client certificate. If a certificate is presented, it is verified at the TLS level during [device authentication](../../security/authentication.md#device-auth-interfaces) (a connection with an untrusted certificate is not established); after successful verification, the server preferentially uses the [authentication token](../../concepts/glossary.md#auth-token) for client authentication, and if no token is present, authentication is performed using the [client certificate](../../security/authentication.md#client-certificate). If no certificate is presented, then when `client_certificate_required: true` is configured, the connection is not established.
- Empty string — the server does not request a client certificate during the TLS handshake; device authentication and client certificate authentication for monitoring are unavailable with this configuration.

The parameter is ignored if TLS is not enabled by the `monitoring_certificate` or `monitoring_certificate_file`+`monitoring_private_key_file` parameters.

Default value: empty string.

||
||

`client_certificate_required`

|

Requirement of a client certificate during TLS-handshake for monitoring.

Possible values:

- `true` — the server requires a client certificate: a connection without a certificate or with an untrusted certificate is not established. `true` can be specified only together with the specified `monitoring_ca_file`.
- `false` — a client certificate is not required; the behavior when requesting a certificate is determined by the `monitoring_ca_file` parameter.

Default value: `false`.

||
|#

Example of enabling mTLS for monitoring: requesting a client certificate without requiring it to be presented.


```yaml
monitoring_config:
  monitoring_certificate_file: /path/to/cert.pem # to enable TLS
  monitoring_private_key_file: /path/to/key.pem
  monitoring_ca_file: /path/to/ca.pem # client certificate request (mTLS)
```


Example of enabling mTLS for monitoring: requiring a client certificate with mandatory presentation.


```yaml
monitoring_config:
  monitoring_certificate_file: /path/to/cert.pem
  monitoring_private_key_file: /path/to/key.pem
  monitoring_ca_file: /path/to/ca.pem
  client_certificate_required: true
```
