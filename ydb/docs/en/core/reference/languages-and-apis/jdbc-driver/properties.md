# JDBC driver properties

The JDBC driver for {{ ydb-short-name }} supports the following configuration properties, which can be specified in the [JDBC URL](#jdbc-url-examples) or passed via additional properties:

* `saFile` — service account key for authentication. The valid value is either the content of the JSON file or a file reference. {#saFile}

* `iamEndpoint` — custom IAM endpoint for authentication using a service account key.

* `token` — token value for authentication. The valid value is either the token content or a token file reference. {#token}

* `useMetadata` — indicates whether to use metadata authentication. Valid values are: {#useMetadata}

    - `true` — use metadata authentication.
    - `false` — do not use metadata authentication.

    Default value: `false`.

* `metadataURL` — custom metadata endpoint.

* `localDatacenter` — the name of the data center local to the application being connected.

* `secureConnection` — indicates whether to use TLS. Valid values are:

    - `true` — enforce TLS.
    - `false` — do not enforce TLS.

    The primary way to indicate whether a connection is secure or not is by using the `grpcs://` scheme for secure connections and `grpc://` for insecure connections in the JDBC URL. This property allows overriding it.

* `secureConnectionCertificate` — custom CA certificate for TLS connections. The valid value is either the certificate content or a certificate file reference.

{% note info %}

File references for `saFile`, `token`, or `secureConnectionCertificate` must be prefixed with the `file:` URL scheme, for example:

* `saFile=file:~/mysakey1.json`
* `token=file:/opt/secret/token-file`
* `secureConnectionCertificate=file:/etc/ssl/cacert.cer`

{% endnote %}

## Using the QueryService mode

By default, the JDBC driver currently uses a legacy API for running queries to be compatible with a broader range of {{ ydb-short-name }} versions. However, that API has some extra [limitations](../../../concepts/limits-ydb.md#query). To turn off this behavior and use a modern API called "Query Service", add the `useQueryService=true` property to the JDBC URL.

## JDBC URL examples {#jdbc-url-examples}

{% include notitle [examples](_includes/jdbc-url-examples.md) %}