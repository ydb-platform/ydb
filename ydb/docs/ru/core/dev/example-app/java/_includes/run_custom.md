Для выполнения примера с использованием любой доступной базы данных YDB вам потребуется знать [эндпоинт](../../../../concepts/connect.md#endpoint) и [путь базы данных](../../../../concepts/connect.md#database).

Если в базе данных включена аутентификация, то вам также понадобится выбрать [режим аутентификации](../../../../concepts/auth.md) и получить секреты - токен или логин/пароль.

Выполните команду по следующему образцу:

```bash
( cd ydb-java-examples/basic_example/target && \
<auth_mode_var>="<auth_mode_value>" java -jar ydb-basic-example.jar <endpoint>?database=<database>)
```

, где

- `<endpoint>` - [эндпоинт](../../../../concepts/connect.md#endpoint).
- `<database>` - [путь базы данных](../../../../concepts/connect.md#database).
- `<auth_mode_var`> - [переменная окружения](../../../../reference/ydb-sdk/auth.md#env), определяющая режим аутентификации.
- `<auth_mode_value>` - значение параметра аутентификации для выбранного режима.

Например:

```bash
( cd ydb-java-examples/basic_example/target && \
YDB_ACCESS_TOKEN_CREDENTIALS="t1.9euelZqOnJuJlc..." java -jar ydb-basic-example.jar grpcs://ydb.example.com:2135?database=/somepath/somelocation)
```
