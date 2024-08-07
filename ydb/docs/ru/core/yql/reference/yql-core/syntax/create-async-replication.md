# CREATE ASYNC REPLICATION

Вызов `CREATE ASYNC REPLICATION` создает экземпляр [асинхронной репликации](../../../concepts/async-replication.md).

## Синтаксис {#syntax}

```sql
CREATE ASYNC REPLICATION <name>
FOR remote_path AS local_path [, another_remote_path AS another_local_path]
WITH (option = value [, ...])
```

где:
* `name` — имя экземпляра асинхронной репликации.
* `remote_path` — относительный или абсолютный путь до исходной таблицы или директории в базе-источнике.
* `local_path` — относительный или абсолютный путь до целевой таблицы или директории в текущей базе.
* `WITH (option = value [, ...])` — [параметры](#params) асинхронной репликации.

### Параметры {#params}

* `CONNECTION_STRING` — [строка соединения](../../../concepts/connect.md#connection_string) c базой-источником. Обязательный параметр.
* Настройки для аутентификации в базе-источнике одним из способов (обязательно):
  * С помощью [токена](../../../recipes/ydb-sdk/auth-access-token.md):
    * `TOKEN_SECRET_NAME` — имя [секрета](../../../concepts/datamodel/secrets.md), содержащего токен.
  * С помощью [логина и пароля](../../../recipes/ydb-sdk/auth-static.md):
    * `USER` — имя пользователя.
    * `PASSWORD_SECRET_NAME` — имя [секрета](../../../concepts/datamodel/secrets.md), содержащего пароль.

## Примеры {#examples}

{% note tip %}

Перед созданием экземпляра асинхронной репликации [создайте](create-object-type-secret.md) секрет с аутентификационными данными для подключения или убедитесь в его существовании и наличии доступа к нему.

{% endnote %}

Создание экземпляра асинхронной репликации для таблицы `table` из базы `/Root/another_database` в текущую базу в таблицу `replica_table`:

```sql
CREATE ASYNC REPLICATION `my_replication_for_single_table`
FOR `table` AS `replica_table`
WITH (
    CONNECTION_STRING = 'grpcs://example.com:2135/?database=/Root/another_database',
    TOKEN_SECRET_NAME = 'my_secret'
);
```

Для подключения к базе `/Root/another_database` используется [эндпоинт](../../../concepts/connect.md#endpoint) `grpcs://example.com:2135`, а для аутентификации — токен из секрета `my_secret`.

Создание экземпляра асинхронной репликации для таблиц `table1` и `table2` в `replica_table1` и `replica_table2`, соответственно:

```sql
CREATE ASYNC REPLICATION `my_replication_for_multiple_tables`
FOR `table1` AS `replica_table1`, `table2` AS `replica_table2`
WITH (
    CONNECTION_STRING = 'grpcs://example.com:2135/?database=/Root/another_database',
    TOKEN_SECRET_NAME = 'my_secret'
);
```

Создание экземпляра асинхронной репликации для содержимого директории `dir`:

```sql
CREATE ASYNC REPLICATION `my_replication_for_dir`
FOR `dir` AS `replica_dir`
WITH (
    CONNECTION_STRING = 'grpcs://example.com:2135/?database=/Root/another_database',
    TOKEN_SECRET_NAME = 'my_secret'
);
```

Создание экземпляра асинхронной репликации для содержимого базы `/Root/another_database`:

```sql
CREATE ASYNC REPLICATION `my_replication_for_database`
FOR `/Root/another_database` AS `/Root/my_database`
WITH (
    CONNECTION_STRING = 'grpcs://example.com:2135/?database=/Root/another_database',
    TOKEN_SECRET_NAME = 'my_secret'
);
```

## См. также

* [ALTER ASYNC REPLICATION](alter-async-replication.md)
* [DROP ASYNC REPLICATION](drop-async-replication.md)
