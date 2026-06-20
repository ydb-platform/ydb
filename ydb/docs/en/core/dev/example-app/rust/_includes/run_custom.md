To run the example against any available {{ ydb-short-name }} database, provide the [endpoint](../../../../concepts/connect.md#endpoint) and the [database path](../../../../concepts/connect.md#database).

If authentication is enabled, choose an [authentication mode](../../../../security/authentication.md) and set the corresponding environment variables.

```bash
( export <auth_mode_var>="<auth_mode_value>" && \
  export YDB_CONNECTION_STRING="<endpoint>/<database>" && \
  cd ydb-rs-sdk/ydb && cargo run --example basic )
```

where

- `<endpoint>`: The [endpoint](../../../../concepts/connect.md#endpoint).
- `<database>`: The [database path](../../../../concepts/connect.md#database).
- `<auth_mode_var>`: The [environment variable](../../../../reference/ydb-sdk/auth.md#env) for the authentication mode.
- `<auth_mode_value>`: The credential value for the selected mode.

For example:

```bash
( export YDB_ACCESS_TOKEN_CREDENTIALS="t1.9euelZqOnJuJlc..." && \
  export YDB_CONNECTION_STRING="grpcs://ydb.example.com:2135/somepath/somelocation" && \
  cd ydb-rs-sdk/ydb && cargo run --example basic )
```
