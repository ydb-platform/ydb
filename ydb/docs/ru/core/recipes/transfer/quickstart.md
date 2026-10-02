# Трансфер — быстрый старт

Эта статья поможет быстро начать работу с [трансфером](../../concepts/transfer.md) в {{ ydb-short-name }} на простейшем модельном примере.

В статье рассматриваются следующие шаги работы с трансфером:

* [создание топика](#step1), из которого будет читать трансфер;
* [создание таблицы](#step2), в которую будут записываться данные;
* [создание трансфера](#step3);
* [заполнение топика данными](#step4);
* [проверка содержимого таблицы](#step5).

## Шаг 1. Создание топика {#step1}

Сначала нужно создать [топик](../../concepts/datamodel/topic.md) в {{ ydb-short-name }}, из которого трансфер будет читать данные. Это можно сделать с помощью [SQL-запроса](../../yql/reference/syntax/create-topic.md):

```yql
CREATE TOPIC `transfer_recipe/source_topic`;
```

Топик `transfer_recipe/source_topic` позволяет передавать любые неструктурированные данные.

## Шаг 2. Создание таблицы {#step2}

После создания топика следует создать [таблицу](../../concepts/datamodel/table.md), в которую будут поступать данные из топика `source_topic`. Это можно сделать с помощью [SQL-запроса](../../yql/reference/syntax/create_table/index.md):

```yql
CREATE TABLE `transfer_recipe/target_table` (
  partition Uint32 NOT NULL,
  offset Uint64 NOT NULL,
  data String,
  PRIMARY KEY (partition, offset)
);
```

Таблица `transfer_recipe/target_table` имеет три столбца:

* `partition` — идентификатор [партиции](../../concepts/glossary.md#partition) топика, из которой получено сообщение;
* `offset` — [порядковый номер](../../concepts/glossary.md#offset), идентифицирующий сообщение внутри партиции;
* `data` — тело сообщения.

## Шаг 3. Создание трансфера {#step3}

После создания топика и таблицы нужно добавить [трансфер](../../concepts/transfer.md) данных, который будет переносить сообщения из топика в таблицу. Это можно сделать с помощью [SQL-запроса](../../yql/reference/syntax/create-transfer.md):

```yql
$transformation_lambda = ($msg) -> {
    return [
        <|
            partition: $msg._partition,
            offset: $msg._offset,
            data: $msg._data
        |>
    ];
};

CREATE TRANSFER `transfer_recipe/example_transfer`
  FROM `transfer_recipe/source_topic` TO `transfer_recipe/target_table`
  USING $transformation_lambda;
```

В этом примере:

* `$transformation_lambda` — это правило преобразования сообщения из топика в колонки таблицы. В данном случае сообщение из топика переносится в таблицу без изменений. Подробнее о настройке правил преобразования вы можете узнать в [документации](../../yql/reference/syntax/create-transfer.md#lambda);
* `$msg` — переменная, содержащая обрабатываемое сообщение из топика.

## Шаг 4. Заполнение топика данными {#step4}

После создания трансфера можно записать в топик сообщения, например, с помощью [{{ ydb-short-name }} CLI](../../reference/ydb-cli/index.md).

{% include [x](../../_includes/ydb-cli-profile.md) %}

```bash
echo "Message 1" | ydb --profile quickstart topic write 'transfer_recipe/source_topic'
echo "Message 2" | ydb --profile quickstart topic write 'transfer_recipe/source_topic'
echo "Message 3" | ydb --profile quickstart topic write 'transfer_recipe/source_topic'
```

## Шаг 5. Проверка содержимого таблицы {#step5}

После записи сообщении в топик `source_topic` спустя некоторое время появятся записи в таблице `transfer_recipe/target_table`. Проверить их наличие можно с помощью [SQL-запроса](../../yql/reference/syntax/select/index.md):

```yql
SELECT *
FROM `transfer_recipe/target_table`;
```

Результат выполнения запроса:

| partition | offset | data |
|-----------|--------|------|
| 0         | 0      | Message 1 |
| 0         | 1      | Message 2 |
| 0         | 2      | Message 3 |

{% include [x](_includes/batching.md) %}

## Заключение

Данная статья приводит простой пример работы с трансфером: создание топика, таблицы и трансфера, записи в топик и проверки результата работы трансфера.

Эти примеры призваны проиллюстрировать синтаксис при работе с трансфером. Более реалистичный пример см. в [статье](nginx.md) описывающей поставку access лога NGINX.

См. также:

* [{#T}](../../concepts/transfer.md)
* [{#T}](nginx.md)
