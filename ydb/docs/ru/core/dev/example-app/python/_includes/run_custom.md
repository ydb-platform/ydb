Для выполнения примера с использованием любой доступной базы данных YDB вам потребуется знать [эндпоинт](../../../../concepts/connect.md#endpoint) и [путь базы данных](../../../../concepts/connect.md#database).

Если в базе данных включена аутентификация, то вам также понадобится выбрать [режим аутентификации](../../../../concepts/auth.md) и получить секреты - токен или логин/пароль.

Выполните команду по следующему образцу:

``` bash
<auth_mode_var>="<auth_mode_value>" \
python3 ydb-python-sdk/examples/basic_example_v1/ -e <endpoint> -d <database>
```

, где

- `<endpoint>` - [эндпоинт](../../../../concepts/connect.md#endpoint).
- `<database>` - [путь базы данных](../../../../concepts/connect.md#database).
- `<auth_mode_var`> - [переменная окружения](../../../../reference/ydb-sdk/auth.md#env), определяющая режим аутентификации.
- `<auth_mode_value>` - значение параметра аутентификации для выбранного режима.

Например:
``` bash
YDB_ACCESS_TOKEN_CREDENTIALS="t1.9euelZqOnJuJlc..." \
python3 ydb-python-sdk/examples/basic_example_v1/ -e grpcs://ydb.example.com:2135 -d /path/db )
```
