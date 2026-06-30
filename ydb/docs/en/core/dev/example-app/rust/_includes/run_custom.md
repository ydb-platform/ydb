To run the example on any available {{ ydb-short-name }} database, specify the [endpoint](../../../../concepts/connect.md#endpoint) and [database path](../../../../concepts/connect.md#database).

If authentication is enabled, select the [authentication mode](../../../../security/authentication.md) and set the environment variables.


```bash
export <auth_mode_var>="<auth_mode_value>"
export YDB_CONNECTION_STRING="<endpoint>/<database>"
cd ydb-rs-sdk/ydb
cargo run --example basic
```


where

- `<endpoint>` — [endpoint](../../../../concepts/connect.md#endpoint).
- `<database>` — [database path](../../../../concepts/connect.md#database).
- `<auth_mode_var>` — [environment variable](../../../../reference/ydb-sdk/auth.md#env) for the authentication mode.
- `<auth_mode_value>` — credential value.

For example:


```bash
export YDB_ACCESS_TOKEN_CREDENTIALS="t1.9euelZqOnJuJlc..."
export YDB_CONNECTION_STRING="grpcs://ydb.example.com:2135/somepath/somelocation"
cd ydb-rs-sdk/ydb
cargo run --example basic
```
