# Connecting to a database

To connect to a {{ ydb-short-name }} database from the {{ ydb-short-name }} CLI or an app running the {{ ydb-short-name }} SDK, specify your [endpoint](#endpoint) and [database path](#database).

## Endpoint {#endpoint}

An endpoint is a string structured as `protocol://host:port` and provided by a {{ ydb-short-name }} cluster owner for proper routing of client queries to its databases by way of a network infrastructure as well as for proper network connections. Cloud databases display the endpoint in the management console on the requisite DB page and also normally send it via the cloud provider's CLI. In corporate environments, endpoint names {{ ydb-short-name }} are provided by the administration team or obtained in the internal cloud management console.

{% include [overlay/endpoint.md](_includes/connect_overlay/endpoint.md) %}

Examples:

* `grpc://localhost:7135` is an unencrypted data interchange protocol (gRPC) with the server running on port 7135 of the same host as the client.
* `grpcs://ydb.example.com` is an encrypted data interchange protocol (gRPCs) with the server running on the ydb.example.com host on an isolated corporate network and listening for connections on YDB default port 2135.
* `grpcs://ydb.serverless.yandexcloud.net:2135` is an encrypted data interchange protocol (gRPCs), public {{ yandex-cloud }} Serverless YDB server at ydb.serverless.yandexcloud.net, port 2135.

## Database path {#database}

Database path (`database`) is a string that defines where the queried database is located in the {{ ydb-short-name }} cluster. Has the [format]{% if lang == "en" %}(https://en.wikipedia.org/wiki/Path_(computing)){% endif %}{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Путь_к_файлу){% endif %} and uses the `/` character as separator. It always starts with a `/`.

A {{ ydb-short-name }} cluster may have multiple databases deployed, with their paths determined by the cluster configuration. Like the endpoint, `database` for cloud databases is displayed in the management console on the desired database page, and can also be obtained via the CLI of the cloud provider.

For cloud solutions, databases are created and hosted on the {{ ydb-short-name }} cluster in self-service mode without the involvement of the cluster owner or administrators.

{% include [overlay/database.md](_includes/connect_overlay/database.md) %}

{% note warning %}

Applications should not in any way interpret the number and value of `database` directories, since they are set in the {{ ydb-short-name }} cluster configuration. When using {{ ydb-short-name }} in {{ yandex-cloud }}, `database` has the format `region_name/cloud_id/database_id`; however, this format may change going forward for new DBs.

{% endnote %}

Examples:

* `/ru-central1/b1g8skpblkos03malf3s/etn01q5ko6sh271beftr` is a {{ yandex-cloud }} database with `etn01q3ko8sh271beftr` as ID deployed in the `b1g8skpbljhs03malf3s` cloud in the `ru-central1` region.
* `/local` is the default database for custom deployment [using Docker](../quickstart.md).

## Connection string {#connection_string}

A connection string is a URL-formatted string that specifies the endpoint and path to a database using the following syntax:

`<endpoint>?database=<database>`

Examples:

- `grpc://localhost:7135?database=/local`
- `grpcs://ydb.serverless.yandexcloud.net:2135?database=/ru-central1/b1g8skpblkos03malf3s/etn01q5ko6sh271beftr`

Using a connection string is an alternative to specifying the endpoint and database path separately and can be used in tools that support this method.

## A root certificate for TLS {#tls-cert}

When using an encrypted protocol ([gRPC over TLS](https://grpc.io/docs/guides/auth/), or gRPCS), a network connection can only be continued if the client is sure that it receives a response from the genuine server that it is trying to connect to, rather than someone in-between intercepting its request on the network. This is assured by verifications through a [chain of trust]{% if lang == "en" %}(https://en.wikipedia.org/wiki/Chain_of_trust){% endif %}{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Цепочка_доверия){% endif %}, for which you need to install a root certificate on your client.

The OS that the client runs on already include a set of root certificates from the world's major certification authorities. However, the {{ ydb-short-name }} cluster owner can use its own CA that is not associated with any of the global ones, which is often the case in corporate environments, and is almost always used for self-deployment of clusters with connection encryption support. In this case, the cluster owner must somehow transfer its root certificate for use on the client side. This certificate may be installed in the operating system's certificate store where the client runs (manually by a user or by a corporate OS administration team) or built into the client itself (as is the case for {{ yandex-cloud }} in {{ ydb-short-name }} CLI and SDK).
