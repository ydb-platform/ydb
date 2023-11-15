# Running {{ ydb-short-name }} in Docker

For debugging or testing, you can run the YDB [Docker](https://docs.docker.com/get-docker/) container.

It is recommended to use Docker 20.10.25 or newer. If you have an older version, you might need to add a `--privileged` or `--security-opt seccomp=unconfined` flag when running containers.

## Connection parameters {#conn}

As a result of completing the instructions below, you'll get a local YDB database that can be accessed using the following:

{% list tabs %}

- gRPC

   - [Endpoint](../../../../concepts/connect.md#endpoint): `grpc://localhost:2136`
   - [Database path](../../../../concepts/connect.md#database): `/local`
   - [Authentication](../../../../concepts/auth.md): Anonymous (no authentication)

- gRPCs/TLS

   - [Endpoint](../../../../concepts/connect.md#endpoint): `grpcs://localhost:2135`
   - [Database path](../../../../concepts/connect.md#database): `/local`
   - [Authentication](../../../../concepts/auth.md): Anonymous (no authentication)

{% endlist %}
