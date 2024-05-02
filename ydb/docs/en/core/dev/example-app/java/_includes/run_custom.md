To run the example against any available YDB database, you need to know the [endpoint](../../../../concepts/connect.md#endpoint) and the [database path](../../../../concepts/connect.md#database).

If authentication is enabled in the database, you also need to select the [authentication mode](../../../../concepts/auth.md) and get secrets (a token or username/password pair).

Run the command as follows:

```bash
( cd ydb-java-examples/basic_example/target && \
<auth_mode_var>="<auth_mode_value>" java -jar ydb-basic-example.jar <endpoint>?database=<database>)
```

where

- `<endpoint>`: The [endpoint](../../../../concepts/connect.md#endpoint).
- `<database>`: The [database path](../../../../concepts/connect.md#database).
- `<auth_mode_var>`: The [environment variable](../../../../reference/ydb-sdk/auth.md#env) that determines the authentication mode.
- `<auth_mode_value>` is the authentication parameter value for the selected mode.

For example:

```bash
( cd ydb-java-examples/basic_example/target && \
YDB_ACCESS_TOKEN_CREDENTIALS="t1.9euelZqOnJuJlc..." java -jar ydb-basic-example.jar grpcs://ydb.example.com:2135?database=/somepath/somelocation)
```
