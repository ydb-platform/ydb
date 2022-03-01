# Using the {{ ydb-short-name }} Docker container

For debugging or testing, you can run the YDB [Docker](https://docs.docker.com/get-docker/) container.

As a result of completing the instructions below, you'll get a local YDB database that can be accessed using the following:

{% list tabs %}

- gRPC
  - [Endpoint ](../../../concepts/connect.md#endpoint): `grpc://localhost:2136`
  - [Database location](../../../concepts/connect.md#database): `/local`
  - [Authentication](../../../concepts/connect.md#auth-modes): Anonymous (without authentication)

- gRPCs/TLS
  - [Endpoint ](../../../concepts/connect.md#endpoint): `grpcs://localhost:2135`
  - [Database location](../../../concepts/connect.md#database): `/local`
  - [Authentication](../../../concepts/connect.md#auth-modes): Anonymous (without authentication)

{% endlist %}

