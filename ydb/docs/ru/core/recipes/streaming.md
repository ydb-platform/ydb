# Рецепты работы со стриминговыми запросами

Эта статья поможет быстро начать работу с [стриминговыми запросами](../concepts/steaming_query/index.md) в {{ ydb-short-name }} на простейшем модельном примере.  Мы будем читать из входного топика сообщения в формате json, фильтровать их, агрегировать и результат записывать в выходной топик.

В статье рассматриваются следующие шаги работы:

* [создание топиков](#step1);
* [создание стримингового запроса](#step2), в которую будут записываться данные;
* [просмотр соостояния запроса](step3)
* [заполнение входного топика данными](#step4);
* [проверка содержимого выходного топика](#step5).

Предворительные условия:

* запущенная база {{ ydb-short-name }}, пример запуска [quick start](../quickstart.md).

## Шаг 1. Создание топиков {#step1}

Сначала нужно создать входной и выходной [топики](../concepts/datamodel/topic.md) в {{ ydb-short-name }}. Их входного стриминговый запрос будет читать данные, в выходной топик будет записывать данные. Это можно сделать с помощью [SQL-запроса](../yql/reference/syntax/create-topic.md):

```yql
CREATE TOPIC `streaming_recipe/input_topic`;
CREATE TOPIC `streaming_recipe/output_topic`;
```

## Шаг 2. Создание стримингового запроса {#step2}

После создания топиков и таблицы нужно запустить стриминговый запрос, который будет обрабатывать сообщения. Это можно сделать с помощью SQL-запроса:

```yql
CREATE STREAMING QUERY `my_queries/query_name` AS
DO BEGIN
    PRAGMA pq.Consumer = "ConsumerName";
    $input = SELECT * FROM `source_name`.`input_topic_name`
    WITH (
        FORMAT = "json_each_row",
        SCHEMA (
            time String NOT NULL,
            level String NOT NULL,
            host String NOT NULL));
    $filtered = SELECT * FROM $input WHERE level = "error";

    $number_errors =
        SELECT COUNT(*) AS error_count, CAST(HOP_START() as String) as ts FROM $filtered
        GROUP BY HoppingWindow(CAST(time AS Timestamp), 'PT600S', 'PT600S', 'PT0S'), host;
    
    $json = SELECT ToBytes(Unwrap(Json::SerializeJson(Yson::From(TableRow())))) FROM $number_errors;
    INSERT INTO `source_name`.`output_topic_name` SELECT * FROM $json;
END DO;
```

## Шаг 3. Просмотр соостояния запроса {#step3}

Состояние запроса можно проверить через системную таблицу [streaming_queries](../dev/system-views.md#streaming_queries)).
Это можно сделать с помощью SQL-запроса:

```yql
SELECT Path, Status, Issues, Text, Run FROM `.sys/streaming_queries`
```

Убедитесь что в поле `Status` значение RUNNING. В противном случае проверьте поле Issues.

## Шаг 4 Заполнение входного топика данными (#step4)

Записать в топик сообщения можно , например, с помощью [{{ ydb-short-name }} CLI](../reference/ydb-cli/index.md).

```bash
echo '{"time": "2025-01-01T00:00:00.000000Z", "level": "error", "host": "host-1"}' | ./ydb --profile quickstart topic write 'streaming_recipe/input_topic'
echo '{"time": "2025-01-01T00:04:00.000000Z", "level": "error", "host": "host-1"}' | ./ydb --profile quickstart topic write 'streaming_recipe/input_topic'
echo '{"time": "2025-01-01T00:08:00.000000Z", "level": "error", "host": "host-1"}' | ./ydb --profile quickstart topic write 'streaming_recipe/input_topic'
echo '{"time": "2025-01-01T00:12:00.000000Z", "level": "error", "host": "host-1"}' | ./ydb --profile quickstart topic write 'streaming_recipe/input_topic'
echo '{"time": "2025-01-01T00:16:00.000000Z", "level": "error", "host": "host-1"}' | ./ydb --profile quickstart topic write 'streaming_recipe/input_topic'
echo '{"time": "2025-01-01T00:20:00.000000Z", "level": "error", "host": "host-1"}' | ./ydb --profile quickstart topic write 'streaming_recipe/input_topic'
```

## Шаг 5 Проверка содержимого выходного топика (#step5)

Прочитать данных из выходного топика можно через cli:

```bash
./ydb --profile quickstart topic read 'streaming_recipe/input_topic' --partition-ids=0 --start-offset 0 --limit 10
```
