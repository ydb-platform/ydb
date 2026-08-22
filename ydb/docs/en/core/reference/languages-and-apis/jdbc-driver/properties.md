# JDBC driver properties

The JDBC driver for {{ ydb-short-name }} supports the following configuration properties, which can be specified in the [JDBC URL](#jdbc-url-examples) or passed via additional properties:

* `saKeyFile` — a path to the service account key file for authentication. {#saKeyFile}

* `iamEndpoint` — custom IAM endpoint for authentication using a service account key.

* `token` — token value for authentication. {#token}

* `tokenFile` — a path to the file with token value for authentication. {#tokenFile}

* `useMetadata` — indicates whether to use metadata authentication. Valid values are: {#useMetadata}

  * `true` — use metadata authentication.
  * `false` — do not use metadata authentication.

  Default value: `false`.

* `metadataURL` — custom metadata endpoint.

* `localDatacenter` — the name of the data center local to the application being connected.

* `secureConnectionCertificate` — a path to the custom CA certificate file for TLS connections.

{% note info %}

The values of the `saKeyFile`, `tokenFile`, or `secureConnectionCertificate` properties can be either absolute paths from the file system root or relative paths from the user's home directory. Examples:

* `saKeyFile=~/mysakey1.json`

* `tokenFile=/opt/secret/token-file`

* `secureConnectionCertificate=/etc/ssl/cacert.cer`

{% endnote %}

## JDBC URL examples {#jdbc-url-examples}

{% include notitle [examples](_includes/jdbc-url-examples.md) %}
