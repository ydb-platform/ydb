To run the example against any available YDB database, you need to know the [endpoint](../../../../../concepts/connect.md#endpoint) and the [database path](../../../../../concepts/connect.md#database).

If authentication is enabled in the database, you also need to select the [authentication mode](../../../../../concepts/auth.md) and get secrets (a token or username/password pair).

Run the command as follows:

{% list tabs %}

- Test app over YDB Table service

```bash
( export <auth_mode_var>="<auth_mode_value>" && cd ydb-go-sdk/examples/basic/native && \
go run ./table -ydb="<endpoint>/<database>" )
```

- Test app over YDB Query service

```bash
( export <auth_mode_var>="<auth_mode_value>" && cd ydb-go-sdk/examples/basic/native && \
go run ./query -ydb="<endpoint>/<database>" )
```

{% endlist %}

where

- `<endpoint>`: The [endpoint](../../../../../concepts/connect.md#endpoint).
- `<database>`: The [database path](../../../../../concepts/connect.md#database).
- `<auth_mode_var>`: The [environment variable](../../../auth.md#env) that determines the authentication mode.
- `<auth_mode_value>` is the authentication parameter value for the selected mode.

For example:

```bash
( export YDB_ACCESS_TOKEN_CREDENTIALS="t1.9euelZqOnJuJlc..." && cd ydb-go-sdk/examples/basic/native && \
go run ./table -ydb="grpcs://ydb.example.com:2135/somepath/somelocation" )
```
