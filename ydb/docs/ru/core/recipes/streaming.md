# Рецепты работы со стриминговыми запросами

Эта статья поможет быстро начать работу с [стриминговыми запросами](../concepts/streaming_query/index.md) в {{ ydb-short-name }} на простейшем модельном примере.  Мы будем читать из входного топика сообщения в формате json, фильтровать их, агрегировать (считать количество ошибок по каждому хосту в интервале 10m) и результат записывать в выходной топик.

В статье рассматриваются следующие шаги работы:

* [создание топиков](#step1);
* [создание внешнего источника данных](#step2);
* [создание стримингового запроса](#step3);
* [просмотр соостояния запроса](step4);
* [заполнение входного топика данными](#step5);
* [проверка содержимого выходного топика](#step6);
* [удаление стримингового запроса](#step7).

Предварительные условия:

* запущенная база {{ ydb-short-name }}, пример запуска [quick start](../quickstart.md).

## Шаг 1. Создание топиков {#step1}

Сначала нужно создать входной и выходной [топики](../concepts/datamodel/topic.md) в {{ ydb-short-name }}. Из входного стриминговый запрос будет читать данные; в выходной топик будет записывать данные. Это можно сделать с помощью [SQL-запроса](../yql/reference/syntax/create-topic.md):

```yql
CREATE TOPIC `streaming_recipe/input_topic`;
CREATE TOPIC `streaming_recipe/output_topic`;
```

## Шаг 2. Создание внешнего источника данных {#step2}

После создания топиков и таблицы нужно создать внешний источник данных. Это можно сделать с помощью SQL-запроса:

```yql
CREATE EXTERNAL DATA SOURCE `source_name` WITH (
    SOURCE_TYPE = "Ydb",
    LOCATION="localhost:2136",
    DATABASE_NAME="/local",
    AUTH_METHOD = "NONE"
);
```

## Шаг 3. Создание стримингового запроса {#step3}

Далее необходимо запустить стриминговый запрос, который будет обрабатывать сообщения. Это можно сделать с помощью SQL-запроса:

```yql
CREATE STREAMING QUERY `my_queries/query_name` AS
DO BEGIN
    $input = SELECT * FROM `source_name`.`streaming_recipe/input_topic`
    WITH (
        FORMAT = "json_each_row",
        SCHEMA (
            time String NOT NULL,
            level String NOT NULL,
            host String NOT NULL));
    $filtered = SELECT * FROM $input WHERE level = "error";

    $number_errors =
        SELECT host, COUNT(*) AS error_count, CAST(HOP_START() as String) as ts FROM $filtered
        GROUP BY HoppingWindow(CAST(time AS Timestamp), 'PT600S', 'PT600S'), host;
    
    $json = SELECT ToBytes(Unwrap(Json::SerializeJson(Yson::From(TableRow())))) FROM $number_errors;
    INSERT INTO `source_name`.`streaming_recipe/output_topic` SELECT * FROM $json;
END DO;
```

## Шаг 4. Просмотр соостояния запроса {#step4}

Состояние запроса можно проверить через системную таблицу [streaming_queries](../dev/system-views.md#streaming_queries).
Это можно сделать с помощью SQL-запроса:

```yql
SELECT Path, Status, Issues, Text, Run FROM `.sys/streaming_queries`
```

Убедитесь что в поле `Status` значение RUNNING. В противном случае проверьте поле `Issues`.

## Шаг 5. Заполнение входного топика данными {#step5}

Записать в топик сообщения можно , например, с помощью [{{ ydb-short-name }} CLI](../reference/ydb-cli/index.md).

```bash
echo '{"time": "2025-01-01T00:00:00.000000Z", "level": "error", "host": "host-1"}' | ./ydb --profile quickstart topic write 'streaming_recipe/input_topic'
echo '{"time": "2025-01-01T00:04:00.000000Z", "level": "error", "host": "host-2"}' | ./ydb --profile quickstart topic write 'streaming_recipe/input_topic'
echo '{"time": "2025-01-01T00:08:00.000000Z", "level": "error", "host": "host-1"}' | ./ydb --profile quickstart topic write 'streaming_recipe/input_topic'
echo '{"time": "2025-01-01T00:12:00.000000Z", "level": "error", "host": "host-2"}' | ./ydb --profile quickstart topic write 'streaming_recipe/input_topic'
```

## Шаг 6. Проверка содержимого выходного топика {#step6}

Прочитать данных из выходного топика можно через cli:

```bash
./ydb --profile quickstart topic read 'streaming_recipe/output_topic' --partition-ids=0 --start-offset 0 --limit 10 --format=newline-delimited
```

Ожидаемый результат:

```yql
{"error_count":1,"host":"host-2","ts":"2025-01-01T00:00:00Z"}
{"error_count":2,"host":"host-1","ts":"2025-01-01T00:00:00Z"}
```

## Шаг 6. Удаление запроса {#step6}

Удалить запрос (при этом он останавливается) можно с помощью SQL-запроса:

```yql
DROP STREAMING QUERY `my_queries/query_name`;
```