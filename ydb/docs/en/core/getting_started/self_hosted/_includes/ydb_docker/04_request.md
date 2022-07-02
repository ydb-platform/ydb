## Making queries {#request}

Install the YDB CLI and execute queries as described in [YDB CLI - Getting started](../../../cli.md), using the endpoint and database location specified at the beginning of this article. For example:

```bash
ydb -e grpc://localhost:2136 -d /local scheme ls
```

To ensure a connection using TLS is successful, add the name of the file with the certificate to the connection parameters. The query in the example below should be executed from the same working directory that you used to start the container:

```bash
ydb -e grpcs://localhost:2135 --ca-file ydb_certs/ca.pem -d /local scheme ls
```

A precompiled version of the [YDB CLI](../../../../reference/ydb-cli/index.md) is also available within the image:

```bash
docker exec <container_id> /ydb -e grpc://localhost:2136 -d /local scheme ls
```

, where

`<container_id>`: The container ID output when you [start](#start) it.

