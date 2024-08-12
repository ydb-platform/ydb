To connect to a locally deployed {{ ydb-short-name }} database according to the [Docker](../../../../quickstart.md) use case, run the following command in the default configuration:

```bash
YDB_ANONYMOUS_CREDENTIALS=1 \
python3 ydb-python-sdk/examples/basic_example_v1/ -e grpc://localhost:2136 -d /local
```

