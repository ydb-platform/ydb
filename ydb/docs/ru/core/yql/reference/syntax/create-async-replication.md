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
* `INITIAL_SCAN`: выполнять ли [первоначальное сканирование](../../../concepts/async-replication.md#initial-scan). Необязательный параметр типа `Bool`. Если не указан, используется настройка сервера; по умолчанию `TRUE`.
  * `TRUE`: исходные данные передаются через потоки изменений, объекты-реплики создаются автоматически.
  * `FALSE`: репликация создаётся в состоянии `PAUSED` и подготавливает потоки изменений без первоначального сканирования. Целевые таблицы не создаются и не подключаются к репликации до перевода в `STANDBY`. Сначала создайте резервную копию источника и восстановите данные в приёмнике, затем запустите репликацию с помощью [ALTER ASYNC REPLICATION](alter-async-replication.md). К моменту запуска целевые таблицы должны существовать и соответствовать схемам источника, включая индексы. Порядок действий описан в [руководстве по первоначальной синхронизации](../../../devops/async-replication-initial-sync.md).
* Настройки для аутентификации в базе-источнике одним из способов (обязательно):

  {% include [x](_includes/async_replication_authentification.md) %}

* `CONSISTENCY_LEVEL` — [уровень согласованности реплицируемых данных](../../../concepts/async-replication.md#consistency-levels):
  * `ROW` — [согласованность данных уровня строки](../../../concepts/async-replication.md#consistency-level-row). Режим по умолчанию.
  * `GLOBAL` — [глобальная согласованность данных](../../../concepts/async-replication.md#consistency-level-global). Дополнительно можно указать:
    * `COMMIT_INTERVAL` — [периодичность фиксации изменений](../../../concepts/async-replication.md#commit-interval) в формате [ISO 8601](https://ru.wikipedia.org/wiki/ISO_8601). Значение по умолчанию — 10 секунд.

## Примеры {#examples}

{% include [x](../_includes/secret_tip.md) %}

Создание экземпляра асинхронной репликации для таблицы `original_table` из базы `/Root/another_database` в текущую базу в таблицу `replica_table`:

```yql
CREATE ASYNC REPLICATION my_replication_for_single_table
FOR original_table AS replica_table
WITH (
    CONNECTION_STRING = 'grpcs://example.com:2135/?database=/Root/another_database',
    TOKEN_SECRET_PATH = 'my_secret'
);
```

Для подключения к базе `/Root/another_database` используется [эндпоинт](../../../concepts/connect.md#endpoint) `grpcs://example.com:2135`, а для аутентификации — токен из секрета `my_secret`.

Создание экземпляра асинхронной репликации для таблиц `original_table_1` и `original_table_2` в `replica_table_1` и `replica_table_2`, соответственно:

```yql
CREATE ASYNC REPLICATION my_replication_for_multiple_tables
FOR original_table_1 AS replica_table_1, original_table_2 AS replica_table_2
WITH (
    CONNECTION_STRING = 'grpcs://example.com:2135/?database=/Root/another_database',
    TOKEN_SECRET_PATH = 'my_secret'
);
```

Создание экземпляра асинхронной репликации для содержимого директории `original_dir`:

```yql
CREATE ASYNC REPLICATION my_replication_for_dir
FOR original_dir AS replica_dir
WITH (
    CONNECTION_STRING = 'grpcs://example.com:2135/?database=/Root/another_database',
    TOKEN_SECRET_PATH = 'my_secret'
);
```

Создание экземпляра асинхронной репликации для содержимого базы `/Root/another_database`:

```yql
CREATE ASYNC REPLICATION my_replication_for_database
FOR `/Root/another_database` AS `/Root/my_database`
WITH (
    CONNECTION_STRING = 'grpcs://example.com:2135/?database=/Root/another_database',
    TOKEN_SECRET_PATH = 'my_secret'
);
```

Создание экземпляра асинхронной репликации с указанием корневого сертификата для TLS:

```yql
CREATE ASYNC REPLICATION my_consistent_replication
FOR original_table AS replica_table
WITH (
    CONNECTION_STRING = 'grpcs://example.com:2135/?database=/Root/another_database',
    TOKEN_SECRET_PATH = 'my_secret',
    CA_CERT = '-----BEGIN CERTIFICATE-----...'
);
```

Создание экземпляра асинхронной репликации в режиме глобальной согласованности данных (периодичность фиксации изменений по умолчанию — 10 секунд):

```yql
CREATE ASYNC REPLICATION my_consistent_replication
FOR original_table AS replica_table
WITH (
    CONNECTION_STRING = 'grpcs://example.com:2135/?database=/Root/another_database',
    TOKEN_SECRET_PATH = 'my_secret',
    CONSISTENCY_LEVEL = 'GLOBAL'
);
```

Создание экземпляра асинхронной репликации в режиме глобальной согласованности данных с минутной периодичностью фиксации изменений:

```yql
CREATE ASYNC REPLICATION my_consistent_replication_1min_commit_interval
FOR original_table AS replica_table
WITH (
    CONNECTION_STRING = 'grpcs://example.com:2135/?database=/Root/another_database',
    TOKEN_SECRET_PATH = 'my_secret',
    CONSISTENCY_LEVEL = 'GLOBAL',
    COMMIT_INTERVAL = Interval('PT1M')
);
```

Создание репликации для [первоначальной синхронизации с помощью резервного копирования и восстановления](../../../devops/async-replication-initial-sync.md):

```yql
CREATE ASYNC REPLICATION my_replication_without_initial_scan
FOR original_table AS replica_table
WITH (
    CONNECTION_STRING = 'grpcs://example.com:2135/?database=/Root/another_database',
    TOKEN_SECRET_PATH = 'my_secret',
    INITIAL_SCAN = FALSE
);
```

Репликация подготавливает потоки изменений в состоянии `PAUSED`. Дождитесь их готовности, выполните резервное копирование и восстановление, затем переведите репликацию в `STANDBY` с помощью [ALTER ASYNC REPLICATION](alter-async-replication.md).

## См. также

* [Первоначальная синхронизация с помощью резервного копирования и восстановления](../../../devops/async-replication-initial-sync.md)
* [ALTER ASYNC REPLICATION](alter-async-replication.md)
* [DROP ASYNC REPLICATION](drop-async-replication.md)
