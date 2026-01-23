# ALTER STREAMING QUERY

`ALTER STREAMING QUERY` изменяет настройки и/или текст [потоковых запросов](../../../concepts/streaming-query.md), а также управляет состоянием запроса (остановкой/запуском).

## Синтаксис

```yql
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

Нужно указать хотя бы одну из настроек `SET` или новый текст потокового запроса.

### Изменение параметров запроса

Команда для изменения параметра потокового запроса выглядит следующим образом:

```yql
ALTER STREAMING QUERY [IF EXISTS] <query_name> SET (<key> = <value>)
```

Доступные параметры:

* `RUN = (TRUE|FALSE)` — запустить или остановить запрос.
* `RESOURCE_POOL = <resource_pool_name>` — имя [пула ресурсов](../../../concepts/glossary.md#resource-pool), в котором будет выполняться запрос.

При выполнении `SET (RUN = TRUE)` — команды запуска потокового запроса, смещения чтения из топика и состояния агрегационных функций будут восстановлены из [чекпоинта](../../../concepts/streaming-query.md#checkpoints). В случае отсутствия чекпоинта запрос будет читать входной поток начиная с самых свежих данных.

Примеры изменения параметров запроса [см. ниже](#parameters-changing-examples).

### Изменение текста запроса

Команда для изменения текста потокового запроса выглядит следующим образом:

```yql
ALTER STREAMING QUERY [IF EXISTS] <query_name> AS
DO BEGIN
    <query_statement>
END DO
```

Где `<query_statement>` — новый текст потокового запроса, ограничения для текста запроса приведены в [{#T}](../../../concepts/streaming-query.md#limitations), примеры текста [см. ниже](#text-changing-examples).

{% note info %}

После изменения текста запроса может быть невозможно восстановить состояния агрегационных функций из [чекпоинта](../../../concepts/streaming-query.md#checkpoints), в этом случае команда изменения текста завершится с ошибкой:

```text
Changing the query text will result in the loss of the checkpoint. Please use FORCE=true to change the request text
```

Чтобы изменить текст запроса со сбросом чекпоинта, необходимо указать настройку `FORCE = TRUE` в блоке `SET`. После такого изменения запрос при запуске восстановит только смещения чтения из топика.

{% endnote %}

{% note warning %}

Поддержка изменения текста запроса с возможностью полного переноса [чекпоинта](../../../concepts/streaming-query.md#checkpoints) находится в разработке, поэтому настройка `FORCE = TRUE` является обязательной при изменении текста.

{% endnote %}

Если не указана явно настройка `RUN` в блоке `SET`, то запрос останется в том же состоянии, в котором он был до изменения текста и будет перезапущен с новым текстом, если был запущен до этого.

Примеры изменения текста запроса [см. ниже](#text-changing-examples).

## Разрешения

Требуется [разрешение](./grant.md#permissions-list) `ALTER SCHEMA` на потоковый запрос, пример выдачи такого разрешения для запроса `my_streaming_query`:

```yql
GRANT ALTER SCHEMA ON my_streaming_query TO `user@domain`
```

## Примеры

### Изменение параметров {#parameters-changing-examples}

Следующая команда остановит запрос с именем `my_streaming_query`:

```yql
ALTER STREAMING QUERY my_streaming_query SET (
    RUN = FALSE
)
```

Следующая команда запустит запрос с именем `my_streaming_query` в [пуле ресурсов](../../../concepts/glossary.md#resource-pool) `my_resource_pool`:

```yql
ALTER STREAMING QUERY my_streaming_query SET (
    RUN = TRUE,
    RESOURCE_POOL = my_resource_pool
)
```

### Изменение текста {#text-changing-examples}

Следующая команда изменит текст запроса с именем `my_streaming_query`, после запуска из [чекпоинта](../../../concepts/streaming-query.md#checkpoints) запроса будут восстановлены только смещения чтения из топика:

```yql
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

Чтобы узнать в каком актуальном статусе находится потоковый запрос, необходимо посмотреть на значение колонки Status в системной таблице [.sys/streaming_queries](../../../dev/system-views.md). Например, выполнив такой запрос:

```yql
SELECT
    Path,
    Status,
    Text,
    Run
FROM
    `.sys/streaming_queries`
```

Возможные значения статуса:

1. `CREATING` — запрос создается в системе после выполнения команды `CREATE STREAMING QUERY`.
2. `CREATED` — запрос был успешно создан, но не запущен, например, в случае создания с указанием `RUN = FALSE`.
3. `RUNNING` — запрос исполняется в системе.
4. `SUSPENDED` — запрос перешел в режим временной остановки в следствие внутренних ошибок. Через некоторое время система автоматически попробует его запустить.
5. `STOPPING` — запрос останавливается пользователем, например, командой `ALTER STREAMING QUERY` с указанием `RUN = FALSE`.
6. `STOPPED` — запрос успешно остановлен пользователем.

Гарантируется, что на момент успешного завершения DDL для создания или изменения потокового запроса, статус будет `CREATED`, `RUNNING`, `STOPPED` или `SUSPENDED` в зависимости от настройки `RUN = (TRUE|FALSE)` и успешности запуска запроса.

Примеры обработки данных в других форматах приведены в статье [{#T}](../../../dev/streaming-query/streaming-query-formats.md). Подробнее о возможностях и ограничениях потоковых запросов [см. в документации](../../../concepts/streaming-query.md).

## См. также

* [{#T}](../../../concepts/streaming-query.md)
* [{#T}](create-streaming-query.md)
* [{#T}](drop-streaming-query.md)
