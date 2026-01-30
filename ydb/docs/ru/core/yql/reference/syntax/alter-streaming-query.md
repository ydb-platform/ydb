# ALTER STREAMING QUERY

`ALTER STREAMING QUERY` изменяет настройки и текст [потоковых запросов](../../../concepts/streaming-query.md), а также управляет их состоянием: запуском и остановкой.

## Синтаксис

```sql
ALTER STREAMING QUERY [IF EXISTS] <query_name> [SET (
    <key1> = <value1>,
    <key2> = <value2>,
    ...
)] [AS
DO BEGIN
    <query_statement1>;
    <query_statement2>;
    ...
END DO]
```

### Параметры

* `IF EXISTS` — не выводить ошибку, если потокового запроса не существует.
* `query_name` — имя потокового запроса, подлежащего изменению.
* `SET (<key> = <value>)` — список настроек потокового запроса, которые нужно обновить, опционально.
* `AS DO BEGIN ... END DO` — новый текст потокового запроса, опционально.

Необходимо указать блок `SET`, новый текст запроса или оба параметра одновременно.

### Изменение параметров запроса

Синтаксис:

```sql
ALTER STREAMING QUERY [IF EXISTS] <query_name> SET (<key> = <value>)
```

Доступные параметры:

* `RUN = (TRUE|FALSE)` — запустить или остановить запрос.
* `RESOURCE_POOL = <resource_pool_name>` — имя [пула ресурсов](../../../concepts/glossary.md#resource-pool), в котором будет выполняться запрос.

При выполнении `SET (RUN = TRUE)` смещения чтения из топика и состояния агрегационных функций восстанавливаются из [чекпоинта](../../../dev/streaming-query/checkpoints.md). При отсутствии чекпоинта чтение начинается с самых свежих данных.

Примеры изменения параметров запроса [см. ниже](#parameters-changing-examples).

### Изменение текста запроса

Синтаксис:

```sql
ALTER STREAMING QUERY [IF EXISTS] <query_name> AS
DO BEGIN
    <query_statement>
END DO
```

Где:

* `<query_statement>` — новый текст потокового запроса. Ограничения приведены в [{#T}](../../../concepts/streaming-query.md#limitations), примеры — [ниже](#text-changing-examples).

{% note warning %}

Поддержка изменения текста запроса с полным переносом [чекпоинта](../../../dev/streaming-query/checkpoints.md) находится в разработке. Настройка `FORCE = TRUE` обязательна.

Если восстановление состояния агрегационных функций невозможно, команда завершится с ошибкой:

```text
Changing the query text will result in the loss of the checkpoint. Please use FORCE=true to change the request text
```

После изменения с `FORCE = TRUE` восстанавливаются только смещения чтения из топика, состояния агрегаций сбрасываются.

{% endnote %}

Если настройка `RUN` не указана в блоке `SET`, запрос сохраняет своё состояние. Работающий запрос будет перезапущен с новым текстом.

Примеры изменения текста запроса [см. ниже](#text-changing-examples).

## Разрешения

Требуется [разрешение](./grant.md#permissions-list) `ALTER SCHEMA` на потоковый запрос, пример выдачи такого разрешения для запроса `my_streaming_query`:

```sql
GRANT ALTER SCHEMA ON my_streaming_query TO `user@domain`
```

## Примеры

### Изменение параметров {#parameters-changing-examples}

Остановка запроса `my_streaming_query`:

```sql
ALTER STREAMING QUERY my_streaming_query SET (
    RUN = FALSE
)
```

Запуск запроса `my_streaming_query` в [пуле ресурсов](../../../concepts/glossary.md#resource-pool) `my_resource_pool`:

```sql
ALTER STREAMING QUERY my_streaming_query SET (
    RUN = TRUE,
    RESOURCE_POOL = my_resource_pool
)
```

### Изменение текста {#text-changing-examples}

Изменение текста запроса `my_streaming_query` со сбросом состояния агрегаций. После запуска из [чекпоинта](../../../dev/streaming-query/checkpoints.md) восстанавливаются только смещения чтения из топика:

```sql
ALTER STREAMING QUERY my_streaming_query SET (
    FORCE = TRUE
) AS
DO BEGIN

INSERT INTO
    ydb_source.output_topic
SELECT
    *
FROM
    ydb_source.input_topic

END DO
```

### Статус запроса {#status-of-query}

Текущий статус запроса доступен в колонке `Status` системной таблицы [.sys/streaming_queries](../../../dev/system-views.md):

```sql
SELECT
    Path,
    Status,
    Text,
    Run
FROM
    `.sys/streaming_queries`
```

Возможные значения статуса:

1. `CREATING` — запрос создаётся после выполнения команды `CREATE STREAMING QUERY`.
2. `CREATED` — запрос создан, но не запущен (например, при указании `RUN = FALSE`).
3. `RUNNING` — запрос выполняется.
4. `SUSPENDED` — запрос приостановлен из-за внутренних ошибок. Система автоматически повторит запуск.
5. `STOPPING` — запрос останавливается по команде `ALTER STREAMING QUERY ... SET (RUN = FALSE)`.
6. `STOPPED` — запрос остановлен.

Гарантируется, что на момент успешного завершения DDL для создания или изменения потокового запроса, статус будет `CREATED`, `RUNNING`, `STOPPED` или `SUSPENDED` в зависимости от настройки `RUN = (TRUE|FALSE)` и успешности запуска запроса.

Примеры обработки данных в других форматах приведены в статье [{#T}](../../../dev/streaming-query/streaming-query-formats.md). Подробнее о возможностях и ограничениях потоковых запросов [см. в документации](../../../concepts/streaming-query.md).

## См. также

* [{#T}](../../../concepts/streaming-query.md)
* [{#T}](create-streaming-query.md)
* [{#T}](drop-streaming-query.md)
