Для запуска примера на любой доступной базе {{ ydb-short-name }} укажите [endpoint](../../../../concepts/connect.md#endpoint) и [путь к базе](../../../../concepts/connect.md#database).

Если включена аутентификация, выберите [режим аутентификации](../../../../security/authentication.md) и задайте переменные окружения.

```bash
export <auth_mode_var>="<auth_mode_value>"
export YDB_CONNECTION_STRING="<endpoint>/<database>"
cd ydb-rs-sdk/ydb
cargo run --example basic
```

где

- `<endpoint>` — [endpoint](../../../../concepts/connect.md#endpoint).
- `<database>` — [путь к базе](../../../../concepts/connect.md#database).
- `<auth_mode_var>` — [переменная окружения](../../../../reference/ydb-sdk/auth.md#env) для режима аутентификации.
- `<auth_mode_value>` — значение учётных данных.

Например:

```bash
export YDB_ACCESS_TOKEN_CREDENTIALS="t1.9euelZqOnJuJlc..."
export YDB_CONNECTION_STRING="grpcs://ydb.example.com:2135/somepath/somelocation"
cd ydb-rs-sdk/ydb
cargo run --example basic
```
