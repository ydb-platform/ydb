To run the example against any available {{ ydb-short-name }} database, the [endpoint](../../../../concepts/connect.md#endpoint) and the [database path](../../../../concepts/connect.md#database) need to be provide.

If authentication is enabled for the database, the [authentication mode](../../../../security/authentication.md) needs to be chosen and credentials (a token or a username/password pair) need to be provided.

Run the command as follows:

```bash
<auth_mode_var>="<auth_mode_value>" \
python3 ydb-python-sdk/examples/basic_example_v1/ -e <endpoint> -d <database>
```

where

- `<endpoint>`: The [endpoint](../../../../concepts/connect.md#endpoint).
- `<database>`: The [database path](../../../../concepts/connect.md#database).
- `<auth_mode_var>`: The [environment variable](../../../../reference/ydb-sdk/auth.md#env) that determines the authentication mode.
- `<auth_mode_value>` is the authentication parameter value for the selected mode.

For example:

```bash
YDB_ACCESS_TOKEN_CREDENTIALS="t1.9euelZqOnJuJlc..." \
python3 ydb-python-sdk/examples/basic_example_v1/ -e grpcs://ydb.example.com:2135 -d /path/db )
```
