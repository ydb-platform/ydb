Для выполнения примера с использованием любой доступной базы данных YDB вам потребуется знать [Эндпоинт](../../../../../concepts/connect.md#endpoint) и [Размещение базы данных](../../../../../concepts/connect.md#database).

Если в базе данных включена аутентификация, то вам также понадобится выбрать [режим аутентификации](../../../../../concepts/connect.md#auth-modes) и получить секреты - токен или логин/пароль.

Выполните команду по следующему образцу:

``` bash
( export <auth_mode_var>="<auth_mode_value>" && cd ydb-go-examples && \
go run ./basic -ydb="<endpoint>?database=<database>" )
```

, где

- `<endpoint>` - [Эндпоинт](../../../../../concepts/connect.md#endpoint)
- `<database>` - [Размещение базы данных](../../../../../concepts/connect.md#database) 
- `<auth_mode_var`> - [Переменная окружения](../../../auth.md#env), определяющая режима аутентификации
- `<auth_mode_value>` - Значение параметра аутентификации для выбранного режима

Например:
``` bash
( export YDB_ACCESS_TOKEN_CREDENTIALS="t1.9euelZqOnJuJlc..." && cd ydb-go-examples && \
go run ./basic -ydb="grpcs://ydb.example.com:2135?database=/somepath/somelocation" )
```

{% include [../../_includes/pars_from_profile_hint.md](../../_includes/pars_from_profile_hint.md) %}