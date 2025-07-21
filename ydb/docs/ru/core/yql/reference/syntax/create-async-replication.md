# CREATE ASYNC REPLICATION

Вызов `CREATE ASYNC REPLICATION` создает экземпляр [асинхронной репликации](../../../concepts/async-replication.md).

## Синтаксис {#syntax}

```yql
CREATE ASYNC REPLICATION <name>
FOR <remote_path> AS <local_path> [, <another_remote_path> AS <another_local_path>]
WITH (option = value [, ...])
```

где:

* `name` — имя экземпляра асинхронной репликации.
* `remote_path` — относительный или абсолютный путь до исходной таблицы или директории в базе-источнике.
* `local_path` — относительный или абсолютный путь до целевой таблицы или директории в текущей базе.
* `WITH (option = value [, ...])` — [параметры](#params) асинхронной репликации.

### Параметры {#params}

* `CONNECTION_STRING` — [строка соединения](../../../concepts/connect.md#connection_string) c базой-источником. Обязательный параметр.
* `CA_CERT` — [корневой сертификат для TLS](../../../concepts/connect.md#tls-cert). Необязательный параметр. Может быть указан, если база-источник поддерживает режим обмена данными с шифрованием (`CONNECTION_STRING` начинается с `grpcs://`).
* Настройки для аутентификации в базе-источнике одним из способов (обязательно):

  * С помощью [токена](../../../recipes/ydb-sdk/auth-access-token.md):

    * `TOKEN_SECRET_NAME` — имя [секрета](../../../concepts/datamodel/secrets.md), содержащего токен.

  * С помощью [логина и пароля](../../../recipes/ydb-sdk/auth-static.md):

    * `USER` — имя пользователя.
    * `PASSWORD_SECRET_NAME` — имя [секрета](../../../concepts/datamodel/secrets.md), содержащего пароль.

* `CONSISTENCY_LEVEL` — [уровень согласованности реплицируемых данных](../../../concepts/async-replication.md#consistency-levels):
  * `ROW` — [согласованность данных уровня строки](../../../concepts/async-replication.md#consistency-level-row). Режим по умолчанию.
  * `GLOBAL` — [глобальная согласованность данных](../../../concepts/async-replication.md#consistency-level-global). Дополнительно можно указать:
    * `COMMIT_INTERVAL` — [периодичность фиксации изменений](../../../concepts/async-replication.md#commit-interval) в формате [ISO 8601](https://ru.wikipedia.org/wiki/ISO_8601). Значение по умолчанию — 10 секунд.

## Примеры {#examples}

{% note tip %}

Перед созданием экземпляра асинхронной репликации [создайте](create-object-type-secret.md) секрет с аутентификационными данными для подключения или убедитесь в его существовании и наличии доступа к нему.

{% endnote %}

Создание экземпляра асинхронной репликации для таблицы `original_table` из базы `/Root/another_database` в текущую базу в таблицу `replica_table`:

```yql
CREATE ASYNC REPLICATION my_replication_for_single_table
FOR original_table AS replica_table
WITH (
    CONNECTION_STRING = 'grpcs://example.com:2135/?database=/Root/another_database',
    TOKEN_SECRET_NAME = 'my_secret'
);
```

Для подключения к базе `/Root/another_database` используется [эндпоинт](../../../concepts/connect.md#endpoint) `grpcs://example.com:2135`, а для аутентификации — токен из секрета `my_secret`.

Создание экземпляра асинхронной репликации для таблиц `original_table_1` и `original_table_2` в `replica_table_1` и `replica_table_2`, соответственно:

```yql
CREATE ASYNC REPLICATION my_replication_for_multiple_tables
FOR original_table_1 AS replica_table_1, original_table_2 AS replica_table_2
WITH (
    CONNECTION_STRING = 'grpcs://example.com:2135/?database=/Root/another_database',
    TOKEN_SECRET_NAME = 'my_secret'
);
```

Создание экземпляра асинхронной репликации для содержимого директории `original_dir`:

```yql
CREATE ASYNC REPLICATION my_replication_for_dir
FOR original_dir AS replica_dir
WITH (
    CONNECTION_STRING = 'grpcs://example.com:2135/?database=/Root/another_database',
    TOKEN_SECRET_NAME = 'my_secret'
);
```

Создание экземпляра асинхронной репликации для содержимого базы `/Root/another_database`:

```yql
CREATE ASYNC REPLICATION my_replication_for_database
FOR `/Root/another_database` AS `/Root/my_database`
WITH (
    CONNECTION_STRING = 'grpcs://example.com:2135/?database=/Root/another_database',
    TOKEN_SECRET_NAME = 'my_secret'
);
```

Создание экземпляра асинхронной репликации в режиме глобальной согласованности данных (периодичность фиксации изменений по умолчанию — 10 секунд):

```yql
CREATE ASYNC REPLICATION my_consistent_replication
FOR original_table AS replica_table
WITH (
    CONNECTION_STRING = 'grpcs://example.com:2135/?database=/Root/another_database',
    TOKEN_SECRET_NAME = 'my_secret',
    CONSISTENCY_LEVEL = 'GLOBAL'
);
```

Создание экземпляра асинхронной репликации в режиме глобальной согласованности данных с минутной периодичностью фиксации изменений:

```yql
CREATE ASYNC REPLICATION my_consistent_replication_1min_commit_interval
FOR original_table AS replica_table
WITH (
    CONNECTION_STRING = 'grpcs://example.com:2135/?database=/Root/another_database',
    TOKEN_SECRET_NAME = 'my_secret',
    CONSISTENCY_LEVEL = 'GLOBAL',
    COMMIT_INTERVAL = Interval('PT1M')
);
```

## См. также

* [ALTER ASYNC REPLICATION](alter-async-replication.md)
* [DROP ASYNC REPLICATION](drop-async-replication.md)
