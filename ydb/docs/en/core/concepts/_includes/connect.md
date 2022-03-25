# Connecting to a database

{% include [overlay/ui.md](connect_overlay/ui.md) %}

To connect to the {{ ydb-short-name }} database from the {{ ydb-short-name }} CLI or an application using the {{ ydb-short-name }} SDK, you'll need to provide the following data:

1. Endpoint.
1. Authentication information.
1. Database location.

## Endpoint {#endpoint}

An endpoint is a string structured as `protocol://host:port` and provided by a {{ ydb-short-name }} cluster owner for proper routing of client queries to its databases by way of a network infrastructure as well as for proper network connections. Cloud databases display the endpoint in the management console on the requisite DB page and also normally send it via the cloud provider's CLI. In corporate environments, endpoint names {{ ydb-short-name }} are provided by the administration team or obtained in the internal cloud management console.

{% include [overlay/endpoint.md](connect_overlay/endpoint.md) %}

Examples:

* `grpc://localhost:7135` is an unencrypted data interchange protocol (gRPC) with the server running on port 7135 of the same host as the client.
* `grpcs://ydb.somecorp-int.ru` is an encrypted data interchange protocol (gRPCs) with the server running on a host called ydb.somecorp-int.ru on the SomeCorp isolated corporate network and listening for connections on YDB default port 2135.
* `grpcs://ydb.serverless.yandexcloud.net:2135` is an encrypted data interchange protocol (gRPCs), public {{ yandex-cloud }} Serverless YDB server at ydb.serverless.yandexcloud.net, port 2135.

{% include [overlay/endpoint_example.md](connect_overlay/endpoint_example.md) %}

## Authentication information {#auth}

Once a network connection is established, the server starts to accept client requests with authentication information for processing. The server uses it to identify the client's account and to verify access to execute the query.

The basic operation model assumes that there is a separate system for managing credentials and permissions: [IAM (Identity and Access Management)](https://en.wikipedia.org/wiki/Identity_management). IAM issues a token linked to the account, which the {{ ydb-short-name }} client passes to the server along with a request. The {{ ydb-short-name }} server accesses IAM to verify the token and permissions to perform the requested operation and caches the result.

{{ ydb-short-name }} also supports username and password based authentication without interacting with IAM, but using this model in practice is limited to simple configurations, primarily local ones.

### Authentication modes {#auth-modes}

{{ ydb-short-name }} clients (CLI or SDK) support the following token selection modes to pass tokens in requests:

* **Anonymous**: Empty token passed in a request.
* **Access Token**: Fixed token set as a parameter for the client (SDK or CLI) and passed in requests.
* **Refresh Token**: [OAuth token](https://auth0.com/blog/refresh-tokens-what-are-they-and-when-to-use-them/) of a user's personal account set as a parameter for the client (SDK or CLI), which the client periodically sends to the IAM API in the background to rotate a token (obtain a new one) to pass in requests.
* **Service Account Key**: Service account attributes and a signature key set as parameters for the client (SDK or CLI), which the client periodically sends to the IAM API in the background to rotate a token (obtain a new one) to pass in requests.
* **Metadata**: Client (SDK or CLI) periodically accessing a local service to rotate a token (obtain a new one) to pass in requests.

Any owner of a valid token can get access to perform operations; therefore, the principal objective of the security system is to ensure that a token remains private and to protect it from being compromised.

Authentication modes with token rotation, such as **Refresh Token** and **Service Account Key**, provide a higher level of security compared to the **Access Token** mode that uses a fixed token, since only secrets with a short validity period are transmitted to the {{ ydb-short-name }} server over the network.

The highest level of security and performance is provided when using the **Metadata** mode, since it eliminates the need to work with secrets when deploying an application and allows accessing the IAM system and caching a token in advance, before running the application.

When choosing the authentication mode among those supported by the server and environment, follow the recommendations below:

* **You would normally use Anonymous** on self-deployed local {{ ydb-short-name }} clusters that are inaccessible over the network.
* **You would use Access Token** when other modes are not supported server side or for setup/debugging purposes. It does not require that the client access IAM. However, if the IAM system supports an API for token rotation, fixed tokens issued by this IAM usually have a short validity period, which makes it necessary to update them manually in the IAM system on a regular basis.
* **Refresh Token** can be used when performing one-time manual operations under a personal account, for example, related to DB data maintenance, performing ad-hoc operations in the CLI, or running applications from a workstation. You can manually obtain this token from IAM once to have it last a long time and save it in an environment variable on a personal workstation to use automatically and with no additional authentication parameters on CLI launch.
* **Service Account Key** is mainly used for applications designed to run in environments where the **Metadata** mode is supported, when testing them outside these environments (for example, on a workstation). It can also be used for applications outside these environments, working as an analog of **Refresh Token** for service accounts. Unlike a personal account, service account access objects and roles can be restricted.
* **Metadata** is used when deploying applications in clouds. Currently, this mode is supported on virtual machines and in {{ sf-name }} {{ yandex-cloud }}.

The token to specify in request parameters can be obtained in the IAM system that the specific {{ ydb-short-name }} deployment is associated with. In particular, {{ ydb-short-name }} in {{ yandex-cloud }} uses Yandex.Passport OAuth and {{ yandex-cloud }} service accounts. When using {{ ydb-short-name }} in a corporate context, a company's standard centralized authentication system may be used.

{% include [overlay/auth_choose.md](connect_overlay/auth_choose.md) %}

When using modes in which the {{ ydb-short-name }} client accesses the IAM system, the IAM URL that provides an API for issuing tokens can be set additionally. By default, existing SDKs and CLIs attempt to access the {{ yandex-cloud }} IAM API hosted at `iam.api.cloud.yandex.net:443`.

## Database location {#database}

Database location (`database`) is a string that defines where the queried database is located in the {{ ydb-short-name }} cluster. Has the format [path to file]{% if lang == "en" %}(https://en.wikipedia.org/wiki/Path_(computing)){% endif %}{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Путь_к_файлу){% endif %} and uses the `/` character as separator. It always starts with a `/`.

A {{ ydb-short-name }} cluster may have multiple databases deployed, and their location is defined by the cluster configuration. Like the endpoint, `database` for cloud databases is displayed in the management console on the desired database page, and can also be obtained via the CLI of the cloud provider.

For cloud solutions, databases are created and hosted on the {{ ydb-short-name }} cluster in self-service mode, with no need in attendance of the cluster owner or administrators.

{% include [overlay/database.md](connect_overlay/database.md) %}

{% note warning %}

Applications should not in any way interpret the number and value of `database` directories, since they are set in the {{ ydb-short-name }} cluster configuration. When using {{ ydb-short-name }} in {{ yandex-cloud }}, `database` has the format `region_name/cloud_id/database_id`; however, this format may change going forward for new DBs.

{% endnote %}

Examples:

* `/ru-central1/b1g8skpblkos03malf3s/etn01q5ko6sh271beftr` is a {{ yandex-cloud }} database with `etn01q3ko8sh271beftr` as ID deployed in the `b1g8skpbljhs03malf3s`cloud in the `ru-central1` region.
* `/local` is the default database for custom deployment [using Docker](../../getting_started/self_hosted/ydb_docker.md).

{% include [overlay/database_example.md](connect_overlay/database_example.md) %}

## Configuring connection parameters on the client {#client-config}

For information about how to define connection parameters on the client, see the following articles:

* [Connecting to and authenticating with a database in the {{ ydb-short-name }} CLI](../../reference/ydb-cli/connect.md).
* [Authentication in the {{ ydb-short-name }} SDK](../../reference/ydb-sdk/auth.md).

## Additional information {#addition}

### A root certificate for TLS {#tls-cert}

When using an encrypted protocol ([gRPC over TLS](https://grpc.io/docs/guides/auth/), or gRPCS), a network connection can only be continued if the client is sure that it receives a response from the genuine server that it is trying to connect to, rather than someone in-between intercepting its request on the network. This is assured by verifications through a [chain of trust]{% if lang == "en" %}(https://en.wikipedia.org/wiki/Chain_of_trust){% endif %}{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Цепочка_доверия ){% endif %} which requires the client to have a root certificate installed to operate.

The OS that the client runs on already include a set of root certificates from the world's major certification authorities. However, the {{ ydb-short-name }} cluster owner can use its own CA that is not associated with any of the global ones, which is often the case in corporate environments, and is almost always used for self-deployment of clusters with connection encryption support. In this case, the cluster owner must somehow transfer its root certificate for use on the client side. This certificate may be installed in the operating system's certificate store where the client runs (manually by a user or by a corporate OS administration team) or built into the client itself (as is the case for {{ yandex-cloud }} in {{ ydb-short-name }} CLI and SDK).

### API for getting IAM tokens {#token-refresh-api}

For token rotation, the {{ ydb-short-name }} SDK and CLI use a gRPC request to the {{ yandex-cloud }} IAM API [IamToken - create]{% if lang == "en" %}(https://cloud.yandex.com/en/docs/iam/api-ref/grpc/iam_token_service#Create){% endif %}{% if lang == "ru" %}(https://cloud.yandex.ru/docs/iam/api-ref/grpc/iam_token_service#Create){% endif %}. In **Refresh Token** mode, the token specified in the OAuth parameter is passed in the `yandex_passport_oauth_token` attribute. In **Service Account Key** mode, a short-lived JWT token is generated from the specified service account attributes and an encryption key and passed in the `jwt` property. The source code for generating the JWT is available as part of the SDK (for example, the `get_jwt()` method in the [Python code](https://github.com/ydb-platform/ydb-python-sdk/blob/main/ydb/iam/auth.py)).

In the {{ ydb-short-name }} SDK and CLI, you can substitute the host used for accessing the API for obtaining tokens. This makes it possible to implement a similar API in corporate contexts.

