# Connecting to a database

{% include [overlay/ui.md](connect_overlay/ui.md) %}

To connect to the {{ ydb-short-name }} database from the {{ ydb-short-name }} CLI or an application using the {{ ydb-short-name }} SDK, you'll need to provide the following data:

1. Endpoint
1. Authentication information
1. Database location

## Endpoint {#endpoint}

The `endpoint` is a string in `protocol://host:port` format provided by the {{ ydb-short-name }} cluster owner to correctly route client requests to its databases via the network infrastructure and to establish a network connection. For cloud databases, the `endpoint` is shown in the management console on the page of the desired database. In addition, you can usually obtain it using the CLI of the cloud provider. In corporate environments, the names of {{ ydb-short-name }}  endpoints are provided by administrators or can also be obtained in the internal cloud management console.

{% include [overlay/endpoint.md](connect_overlay/endpoint.md) %}

Examples:

- `grpc://localhost:7135` indicates a data exchange protocol without encryption (gRPC), the server is run on the same host as the client and accepts connections on port 7135.
- `grpcs://ydb.somecorp-int.ru` indicates a data exchange protocol with encryption (gRPCs), the server is run on the host ydb.somecorp-int.ru in the isolated SomeCorp corporate intranet and accepts connections on the YDB default port 2135.
- `grpcs://ydb.serverless.yandexcloud.net:2135` indicates a data exchange protocol with encryption (gRPCs), the public server Serverless YDB Yandex.Cloud ydb.serverless.yandexcloud.net, and port 2135.

{% include [overlay/endpoint_example.md](connect_overlay/endpoint_example.md) %}

## Authentication information {#auth}

Once a network connection is established, the server starts to accept client requests with authentication information for processing. Based on this information, the server identifies the client's account and verifies permission to execute the request.

The basic operation model assumes that there is a separate system for managing credentials and permissions: [IAM (Identity and Access Management)](https://en.wikipedia.org/wiki/Identity_management). IAM issues a token linked to the account, which the {{ ydb-short-name }} client passes to the server along with a request. The {{ ydb-short-name }} server accesses the IAM system to verify the token and permission to perform the requested operation and caches the result.

{{ ydb-short-name }} also supports username and password based authentication without interacting with IAM, but using this model in practice is limited to simple configurations, primarily local ones.

### Authentication modes {#auth-modes}

{{ ydb-short-name }} clients (CLI or SDK) support the following token selection modes to pass tokens in requests:

- **Anonymous**: An empty token is passed in a request.
- **Access Token**: A fixed token is set as a parameter for an SDK or CLI and passed in a request.
- **Refresh Token**: An [OAuth token](https://auth0.com/blog/refresh-tokens-what-are-they-and-when-to-use-them/) of a user's personal account is set as a parameter for an SDK or CLI, using which the client regularly accesses the IAM API in the background for rotating (obtaining a new) token that is passed in a request.
- **Service Account Key**: The service account attributes and the signing key are set as a parameter for an SDK or CLI, using which the client regularly accesses the IAM API in the background for rotating (obtaining a new) token that is passed in a request.
- **Metadata**: An SDK or CLI regularly accesses the local service for rotating (obtaining a new) token that is passed in a request.

Any owner of a valid token can get permission to perform operations, so the main task of the security system is to ensure the token's privacy and not to compromise it.

Authentication modes with token rotation, such as **Refresh Token** and **Service Account Key**, provide a higher level of security compared to the **Access Token** mode that uses a fixed token, since only secrets with a short validity period are transmitted to the {{ ydb-short-name }} server over the network.

The highest level of security and performance is provided when using the **Metadata** mode, since it eliminates the need to work with secrets when deploying an application and allows accessing the IAM system and caching a token in advance, before running the application.

When choosing the authentication mode among those supported by the server and environment, follow the recommendations below:

- **Anonymous** is usually used on self-deployed local {{ ydb-short-name }} clusters that are inaccessible over the network.
- **Access Token** is used when other modes are not supported on the server side or for setup/debugging purposes. It does not require client interactions with the IAM system. However, if the IAM system supports an API for token rotation, fixed tokens issued by this IAM usually have a short validity period, which makes it necessary to update them manually in the IAM system on a regular basis.
- **Refresh Token** can be used when performing one-time manual operations under a personal account, for example, related to DB data maintenance, performing ad-hoc operations in the CLI, or running applications from a workstation. You can manually obtain this token once in the IAM system for a long period and store it in an environment variable on a personal workstation to be used automatically and without additional authentication parameters when starting the CLI.
- **Service Account Key** is mainly used for applications designed to run in environments where the **Metadata** mode is supported, when testing them outside these environments (for example, on a workstation). It can also be used for applications outside these environments, working as an analog of **Refresh Token** for service accounts. Unlike a personal account, access objects and service account roles can be restricted.
- **Metadata** is used when deploying applications in clouds. Currently, this mode is supported on VMs and in Yandex Cloud Functions.

The token to specify in request parameters can be obtained in the IAM system that the specific {{ ydb-short-name }} deployment is associated with. In particular, for the {{ ydb-short-name }} service, Yandex.Cloud uses Yandex.Passport OAuth and Yandex.Cloud service accounts. When using {{ ydb-short-name }} in a corporate context, centralized authentication systems that are standard for the company can be used.

{% include [overlay/auth_choose.md](connect_overlay/auth_choose.md) %}

When using modes in which the {{ ydb-short-name }} client accesses the IAM system, the IAM URL that provides an API for issuing tokens can be set additionally. By default, the existing SDK and CLI make an attempt to access the Yandex.Cloud IAM API hosted at `iam.api.cloud.yandex.net:443`.

## Database location {#database}

Database location (`database`) is a string that defines where the queried database is located in the {{ ydb-short-name }} cluster. Represented as a [file path](https://en.wikipedia.org/wiki/Path_(computing)) with `/` used as a separator. It always starts with a `/`.

A {{ ydb-short-name }} cluster may have multiple databases deployed, and their location is defined by the cluster configuration. Like the endpoint, `database` for cloud databases is displayed in the management console on the desired database page, and can also be obtained via the CLI of the cloud provider.

For cloud solutions, databases are created and hosted on the {{ ydb-short-name }} cluster in self-service mode, with no need in attendance of the cluster owner or administrators.

{% include [overlay/database.md](connect_overlay/database.md) %}

{% note warning %}

Applications should not in any way interpret the number and value of `database` directories, since they are set in the {{ ydb-short-name }} cluster configuration. For example, when working with {{ ydb-short-name }} in Yandex.Cloud, the `database` current structure is `region_name/cloud_id/database_id`. However, this format may be changed in the future for new databases.

{% endnote %}

Examples:

- `/ru-central1/b1g8skpblkos03malf3s/etn01q5ko6sh271beftr` is the Yandex.Cloud database with the `etn01q3ko8sh271beftr` ID in the `b1g8skpbljhs03malf3s` cloud, deployed in the `ru-central1` region.
- `/local` is the default database for custom deployment [using Docker](../../getting_started/ydb_docker.md)

{% include [overlay/database_example.md](connect_overlay/database_example.md) %}

## Configuring connection parameters on the client {#client-config}

For information about how to define connection parameters on the client, see the following articles:

* [Connecting to and authenticating with a database in the {{ ydb-short-name }} CLI](../../reference/ydb-cli/connect.md)
* [Authentication in the {{ ydb-short-name }} SDK](../../reference/ydb-sdk/auth.md)

## Additional information {#addition}

### A root certificate for TLS {#tls-cert}

When using an encrypted protocol ([gRPC over TLS](https://grpc.io/docs/guides/auth/), or gRPCS), a network connection can only be continued if the client is sure that it receives a response from the genuine server that it is trying to connect to, rather than someone in-between intercepting its request on the network. This is ensured by verifications through a [chain of trust](https://en.wikipedia.org/wiki/Chain_of_trust), to enable which, the client needs to have a root certificate installed.

The OS that the client runs on already include a set of root certificates from the world's major certification authorities. However, the {{ ydb-short-name }} cluster owner can use its own CA that is not associated with any of the global ones, which is often the case in corporate environments, and is almost always used for self-deployment of clusters with connection encryption support. In this case, the cluster owner must somehow transfer its root certificate for use on the client side. This certificate can be installed in the certificate store of the OS where the client is run (manually by a user or a corporate team of OS admins), or embedded in the client itself (as in the {{ ydb-short-name }} CLI and SDK  Yandex.Cloud).

### API for getting IAM tokens {#token-refresh-api}

To rotate tokens, the {{ ydb-short-name }} SDK and CLI use a gRPC request to the Yandex.Cloud IAM API: [IamToken - create](https://cloud.yandex.com/en/docs/iam/api-ref/grpc/iam_token_service#Create). In **Refresh Token** mode, the token specified in the OAuth parameter is passed in the `yandex_passport_oauth_token` attribute. In **Service Account Key** mode, based on the specified attributes of the service account and encryption key, a JSON Web Token (JWT) with a short validity period is generated and passed in the `jwt` attribute. The source code for generating the JWT is available as part of the SDK (for example, the `get_jwt()` method in the [Python code](https://github.com/ydb-platform/ydb-python-sdk/blob/main/ydb/iam/auth.py)).

In the {{ ydb-short-name }} SDK and CLI, you can substitute the host used for accessing the API for obtaining tokens. This makes it possible to implement a similar API in corporate contexts.

