## Make a query to the database {#request}

Make a query to the {{ ydb-short-name }} database in a Docker container:

```bash
ydb \
  -e grpcs://localhost:2135 \
  --ca-file $(pwd)/ydb_certs/ca.pem \
  -d /local table query execute -q 'select 1;'
```

Where:

* `-e`: Database endpoint.
* `--ca-file`: Path to the TLS certificate.
* `-d`: DB name and query parameters.

Output:

```text
┌─────────┐
| column0 |
├─────────┤
| 1       |
└─────────┘
```

A precompiled version of the [YDB CLI](../../../reference/ydb-cli/index.md) is also available within the image:

```bash
sudo docker exec <CONTAINER-ID> /ydb -e localhost:2135 -d /local table query execute -q 'select 1;'
┌─────────┐
| column0 |
├─────────┤
| 1       |
└─────────┘
```