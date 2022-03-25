To connect to a locally deployed YDB database according to the [Docker](../../../../../getting_started/self_hosted/ydb_docker.md) use case, run the following command in the default configuration:

```bash
YDB_ANONYMOUS_CREDENTIALS=1 \
python3 ydb-python-sdk/examples/basic_example_v1/ -e grpc://localhost:2136 -d /local
```

