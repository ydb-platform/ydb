# Быстрый старт: чтение и запись в топики

В этом руководстве вы создадите свой первый [потоковый запрос](../../concepts/streaming-query.md).

Запрос будет:

- читать события из входного [топика](../../concepts/datamodel/topic.md);
- отбирать только ошибки;
- подсчитывать количество ошибок по каждому серверу за 10 минут;
- записывать результат в выходной топик.

События поступают в формате JSON с полями: время, уровень логирования и имя сервера.

Вы выполните следующие шаги:

* [создание топиков](#step1);
* [создание внешнего источника данных](#step2);
* [создание потокового запроса](#step3);
* [просмотр состояния запроса](#step4);
* [заполнение входного топика данными](#step5);
* [проверка содержимого выходного топика](#step6);
* [удаление потокового запроса](#step7).

## Предварительные условия {#requirements}

Для выполнения примеров вам потребуется:

* запущенная база {{ ydb-short-name }} — см. [quick start](../../quickstart.md);
* включённые флаги `enable_external_data_sources` и `enable_streaming_queries`.

{% list tabs %}

- Docker

  ```bash
  docker run -d --rm --name ydb-local -h localhost \
    --platform linux/amd64 \
    -p 2135:2135 -p 2136:2136 -p 8765:8765 -p 9092:9092 \
    -v $(pwd)/ydb_certs:/ydb_certs \
    -e GRPC_TLS_PORT=2135 -e GRPC_PORT=2136 -e MON_PORT=8765 \
    -e YDB_FEATURE_FLAGS=enable_external_data_sources,enable_streaming_queries \
    ydbplatform/local-ydb:trunk
  ```

- local_ydb

  ```bash
  ./local_ydb deploy \
    --ydb-working-dir=/absolute/path/to/working/directory \
    --ydb-binary-path=/path/to/kikimr/driver \
    --enable-feature-flag=enable_external_data_sources \
    --enable-feature-flag=enable_streaming_queries
  ```

{% endlist %}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

## Шаг 1. Создание топиков {#step1}

Создайте входной и выходной [топики](../../concepts/datamodel/topic.md):

```sql
CREATE TOPIC input_topic;
CREATE TOPIC output_topic;
```

Проверьте, что топики созданы:

```bash
./ydb --profile quickstart scheme ls
```

## Шаг 2. Создание внешнего источника данных {#step2}

Создайте [внешний источник данных](../../concepts/datamodel/external_data_source.md) с помощью [CREATE EXTERNAL DATA SOURCE](../../yql/reference/syntax/create-external-data-source.md):

```sql
CREATE EXTERNAL DATA SOURCE ydb_source WITH (
    SOURCE_TYPE = "Ydb",
    LOCATION = "localhost:2136",
    DATABASE_NAME = "/local",
    AUTH_METHOD = "NONE"
);
```

{% note info %}

Укажите значения `LOCATION` и `DATABASE_NAME`, соответствующие вашей базе {{ ydb-short-name }}.

{% endnote %}

## Шаг 3. Создание потокового запроса {#step3}

Создайте [потоковый запрос](../../concepts/streaming-query.md) с помощью [CREATE STREAMING QUERY](../../yql/reference/syntax/create-streaming-query.md):

```sql
CREATE STREAMING QUERY query_example AS
DO BEGIN

PRAGMA ydb.DqChannelVersion = "1";

$number_errors = SELECT
    Host,
    COUNT(*) AS ErrorCount,
    CAST(HOP_START() AS String) AS Ts  -- Время начала окна HOP соответствующего результату агрегации
FROM
    ydb_source.input_topic
WITH (
    FORMAT = json_each_row,
    SCHEMA = (
        Time String NOT NULL,
        Level String NOT NULL,
        Host String NOT NULL
    )
)
WHERE
    Level = "error"
GROUP BY
    HOP(CAST(Time AS Timestamp), "PT600S", "PT600S", "PT0S"),  -- Число ошибок на неперекрывающихся окнах длиной 10 минут
    Host;

INSERT INTO
    ydb_source.output_topic
SELECT
    ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))  -- Serialize all columns into JSON
FROM
    $number_errors

END DO
```

Подробнее:

- Агрегация `GROUP BY HOP` — [{#T}](../../yql/reference/syntax/select/group-by.md#group-by-hop).
- Запись данных в топик — [{#T}](../../dev/streaming-query/streaming-query-formats.md#write_formats).

## Шаг 4. Просмотр состояния запроса {#step4}

Проверьте состояние запроса через системную таблицу [streaming_queries](../../dev/system-views.md#streaming_queries):

```sql
SELECT
    Path,
    Status,
    Issues,
    Run
FROM
    `.sys/streaming_queries`
```

Убедитесь, что в поле `Status` значение `RUNNING`. В противном случае проверьте поле `Issues`.

Если запрос находится в статусе `SUSPENDED` или в поле `Issues` есть ошибки, обратитесь к разделу диагностика ошибок.


## Шаг 5. Заполнение входного топика данными {#step5}

Запишите тестовые сообщения в топик с помощью [{{ ydb-short-name }} CLI](../../reference/ydb-cli/index.md):

```bash
echo '{"Time": "2025-01-01T00:00:00.000000Z", "Level": "error", "Host": "host-1"}' | ./ydb --profile quickstart topic write input_topic
echo '{"Time": "2025-01-01T00:04:00.000000Z", "Level": "error", "Host": "host-2"}' | ./ydb --profile quickstart topic write input_topic
echo '{"Time": "2025-01-01T00:08:00.000000Z", "Level": "error", "Host": "host-1"}' | ./ydb --profile quickstart topic write input_topic
echo '{"Time": "2025-01-01T00:12:00.000000Z", "Level": "error", "Host": "host-2"}' | ./ydb --profile quickstart topic write input_topic
echo '{"Time": "2025-01-01T00:12:00.000000Z", "Level": "error", "Host": "host-1"}' | ./ydb --profile quickstart topic write input_topic
```

Результат появится в выходном топике после закрытия 10-минутного окна агрегации.

## Шаг 6. Проверка содержимого выходного топика {#step6}

Прочитайте данные из выходного топика:

```bash
./ydb --profile quickstart topic read output_topic --partition-ids 0 --start-offset 0 --limit 10 --format newline-delimited
```

Ожидаемый результат:

```json
{"ErrorCount":1,"Host":"host-2","Ts":"2025-01-01T00:00:00Z"}
{"ErrorCount":2,"Host":"host-1","Ts":"2025-01-01T00:00:00Z"}
```

## Шаг 7. Удаление запроса {#step7}

Удалите запрос с помощью [DROP STREAMING QUERY](../../yql/reference/syntax/drop-streaming-query.md):

```sql
DROP STREAMING QUERY query_example;
```

## Что дальше {#next-steps}

- Изучите [форматы данных](../../dev/streaming-query/streaming-query-formats.md), поддерживаемые в потоковых запросах;
- узнайте, как [обогащать данные из S3](../../dev/streaming-query/S3-enrichment.md);
- научитесь [записывать результаты в таблицы](../../dev/streaming-query/table-writing.md).

## См. также

* [{#T}](../../concepts/streaming-query.md);
* [{#T}](../../dev/streaming-query/streaming-query-formats.md).
