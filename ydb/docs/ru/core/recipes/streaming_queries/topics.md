# Чтение/запись локальных топиков

Эта статья поможет быстро начать работу с [потоковыми запросами](../../concepts/streaming_query/index.md) в {{ ydb-short-name }} на простейшем модельном примере. Мы будем считать количество ошибок по каждому хосту в интервале 10m. Для этого будем читать из входного топика сообщения в формате JSON, фильтровать их, агрегировать и результат записывать в выходной топик.

В статье рассматриваются следующие шаги работы:

* [создание топиков](#step1);
* [создание внешнего источника данных](#step2);
* [создание потокового запроса](#step3);
* [просмотр состояния запроса](#step4);
* [заполнение входного топика данными](#step5);
* [проверка содержимого выходного топика](#step6);
* [удаление потокового запроса](#step7).

## Предварительные условия {#requirements}

* запущенная база {{ ydb-short-name }}, пример запуска [quick start](../../quickstart.md),
* включены флаги `enable_external_data_sources` и `enable_streaming_queries`:

  * если вы запускаете {{ ydb-short-name }} через docker, то передайте флаги в `docker run`:

    ```bash
    docker run -d --rm --name ydb-local -h localhost \
      --platform linux/amd64 \
      -p 2135:2135 -p 2136:2136 -p 8765:8765 -p 9092:9092 \
      -v $(pwd)/ydb_certs:/ydb_certs \
      -e GRPC_TLS_PORT=2135 -e GRPC_PORT=2136 -e MON_PORT=8765 \
      -e YDB_FEATURE_FLAGS=enable_external_data_sources,enable_streaming_queries \
      ydbplatform/local-ydb:trunk
    ```

  * если вы запускаете {{ ydb-short-name }} через `local_ydb`, то передайте флаги в `deploy`:

    ```bash
    ./local_ydb deploy --ydb-working-dir=/absolute/path/to/working/directory --ydb-binary-path=/path/to/kikimr/driver --enable-feature-flag=enable_external_data_sources --enable-feature-flag=enable_streaming_queries
    ```

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

## Шаг 1. Создание топиков {#step1}

Сначала нужно создать входной и выходной [топики](../../concepts/datamodel/topic.md) в {{ ydb-short-name }}. Из входного потоковый запрос будет читать данные; в выходной топик будет записывать данные. Это можно сделать с помощью [SQL-запроса](../../yql/reference/syntax/create-topic.md):

```yql
CREATE TOPIC `streaming_test/input_topic`;
CREATE TOPIC `streaming_test/output_topic`;
```

## Шаг 2. Создание внешнего источника данных {#step2}

После создания топиков нужно создать внешний источник данных. Это можно сделать с помощью SQL-запроса:

```yql
CREATE EXTERNAL DATA SOURCE `streaming_test/ydb_source` WITH (
    SOURCE_TYPE = 'Ydb',
    LOCATION = 'localhost:2136',
    DATABASE_NAME = '/local',
    AUTH_METHOD = 'NONE'
);
```

## Шаг 3. Создание потокового запроса {#step3}

Далее необходимо запустить потоковый запрос. Это можно сделать с помощью SQL-запроса:

```yql
CREATE STREAMING QUERY `streaming_test/query_name` AS
DO BEGIN
$input = (
    SELECT
        *
    FROM
        `streaming_test/ydb_source`.`streaming_test/input_topic` WITH (
            FORMAT = 'json_each_row',
            SCHEMA = (time String NOT NULL, level String NOT NULL, host String NOT NULL)
        )
);

$filtered = (
    SELECT
        *
    FROM
        $input
    WHERE
        level == 'error'
);

$number_errors = (
    SELECT
        host,
        COUNT(*) AS error_count,
        CAST(HOP_START() AS String) AS ts
    FROM
        $filtered
    GROUP BY HOP(CAST(time AS Timestamp), 'PT600S', 'PT600S', 'PT0S'), host
);

$json = (
    SELECT
        ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
    FROM
        $number_errors
);

INSERT INTO `streaming_test/ydb_source`.`streaming_test/output_topic`
SELECT
    *
FROM
    $json
;
END DO;
```

## Шаг 4. Просмотр состояния запроса {#step4}

Состояние запроса можно проверить через YDB UI во вкладке диагностики по клику на потоковый запрос или альтернативно через системную таблицу [streaming_queries](../../dev/system-views.md#streaming_queries).
Это можно сделать с помощью SQL-запроса:

```yql
SELECT Path, Status, Issues, Run FROM `.sys/streaming_queries`
;
```

Убедитесь что в поле `Status` значение RUNNING. В противном случае проверьте поле `Issues`.

## Шаг 5. Заполнение входного топика данными {#step5}

Записать в топик сообщения можно, например, с помощью [{{ ydb-short-name }} CLI](../../reference/ydb-cli/index.md).

```bash
echo '{"time": "2025-01-01T00:00:00.000000Z", "level": "error", "host": "host-1"}' | ./ydb --profile quickstart topic write 'streaming_test/input_topic'
echo '{"time": "2025-01-01T00:04:00.000000Z", "level": "error", "host": "host-2"}' | ./ydb --profile quickstart topic write 'streaming_test/input_topic'
echo '{"time": "2025-01-01T00:08:00.000000Z", "level": "error", "host": "host-1"}' | ./ydb --profile quickstart topic write 'streaming_test/input_topic'
echo '{"time": "2025-01-01T00:12:00.000000Z", "level": "error", "host": "host-2"}' | ./ydb --profile quickstart topic write 'streaming_test/input_topic'
echo '{"time": "2025-01-01T00:12:00.000000Z", "level": "error", "host": "host-1"}' | ./ydb --profile quickstart topic write 'streaming_test/input_topic'
```

## Шаг 6. Проверка содержимого выходного топика {#step6}

Данные в выходном топике можно просмотреть через YDB UI (кликнув на иконку `Open Preview` на топике).

Также прочитать данные из выходного топика можно через cli (читаем партицию с номером 0 c нулевого смещения):

```bash
./ydb --profile quickstart topic read 'streaming_test/output_topic' --partition-ids=0 --start-offset 0 --limit 10 --format=newline-delimited
```

Ожидаемый результат:

```yql
{"error_count":1,"host":"host-2","ts":"2025-01-01T00:00:00Z"}
{"error_count":2,"host":"host-1","ts":"2025-01-01T00:00:00Z"}
```

## Шаг 7. Удаление запроса {#step7}

Остановить и удалить запрос можно помощью SQL запроса:

```yql
DROP STREAMING QUERY `streaming_test/query_name`;
```
