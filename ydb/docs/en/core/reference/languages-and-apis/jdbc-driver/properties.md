# JDBC driver properties

The JDBC driver for {{ ydb-short-name }} supports the following configuration properties, which can be specified in the [JDBC URL](#jdbc-url-examples) or passed via extra properties:

* `saFile` — service account key for authentication. The valid value is the content of the JSON file or the file reference. {#saFile}

* `iamEndpoint` — custom IAM endpoint for authentication via a service account key.

* `token` — token value for authentication. The valid value is the token content or the token file reference. {#token}

* `useMetadata` — indicates whether to use metadata authentication. Valid values are: {#useMetadata}

    - `true` — use metadata authentication.
    - `false` — do not use metadata authentication.

    Default value: `false`.

* `metadataURL` — custom metadata endpoint.

* `localDatacenter` — name of the data center, which is local to the application being connected.

* `secureConnection` — indicates whether to use TLS. Valid values are:

    - `true` — enforce TLS.
    - `false` — do not enforce TLS.

    Usually you indicate whether a connection is secure or not by using the `grpcs://` scheme for secure connections and `grpc://` for insecure connections in the JDBC URL.

* `secureConnectionCertificate` — custom CA certificate for TLS connections. The valid value is the certificate content or the certificate file reference.

{% note info %}

File references for `saFile`, `token` or `secureConnectionCertificate` must be prefixed with the `file:` URL scheme, for example:

* `saFile=file:~/mysaley1.json`

* `token=file:/opt/secret/token-file`

* `secureConnectionCertificate=file:/etc/ssl/cacert.cer`

{% endnote %}

## Using the QueryService mode

By default, the JDBC driver for {{ ydb-short-name }} uses TableService to execute all queries. However, TableService has some [limitations](../../../concepts/limits-ydb.md#query).

To avoid these limitations, you can try a new experimental QueryService mode. To enable the QueryService mode, use the `useQueryService=true` property in the JDBC URL.

## JDBC URL Examples {#jdbc-url-examples}

{% include notitle [examples](_includes/jdbc-url-examples.md) %}