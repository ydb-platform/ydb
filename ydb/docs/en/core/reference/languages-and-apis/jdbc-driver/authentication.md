# Authentication modes

The JDBC Driver for {{ ydb-short-name }} supports the following [authentication modes](../../ydb-sdk/auth.md):

* `Anonymous` is used when a username and password are not specified and no other authentication properties are configured. No authentication is provided.

* `Static Credentials` is used when a username and password are specified.

* `Access Token` is used when the [`token`](properties.md#token) property is configured. This authentication method requires a {{ ydb-short-name }} authentication token, which you can get by executing the following {{ ydb-short-name }} CLI command: `ydb auth get-token`.

* `Metadata` is used when the [`useMetadata`](properties.md#useMetadata) property is set to `true`. This method extracts the authentication data from the metadata of a virtual machine, serverless container or a serverless function running in a cloud environment.

* `Service Account Key` is used when the [`saFile`](properties.md#saFile) property is configured. This method extracts the service account key and uses it for authentication.