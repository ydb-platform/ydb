## Making queries {#request}

[Install](../../../../reference/ydb-cli/install.md) the YDB CLI and run a query, for example:

```bash
ydb -e grpc://localhost:2136 -d /local scheme ls
```

To ensure a connection using TLS is successful, add the name of the file with the certificate to the connection parameters. The query in the example below should be executed from the same working directory that you used to start the container:

```bash
ydb -e grpcs://localhost:2135 --ca-file ydb_certs/ca.pem -d /local scheme ls
```

A pre-built [YDB CLI](../../../../reference/ydb-cli/index.md) version is also available within the image:

```bash
docker exec <container_id> /ydb -e grpc://localhost:2136 -d /local scheme ls
```

, where

`<container_id>`: Container ID that is output when you [start](#start) the container.