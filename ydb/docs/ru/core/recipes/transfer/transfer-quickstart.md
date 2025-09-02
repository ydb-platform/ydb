# Трансфер — быстрый старт

Эта статья поможет быстро начать работу с [трансфером](../../concepts/transfer.md) в {{ ydb-short-name }} на простейшем модельном примере.

В статье будут рассмотрены следующие шаги работы с трансфером:

* [создание топика](#step1) из которого будет читать трансфер;
* [создание таблицы](#step2) в которую будут записываться данные;
* [создание трансфера](#step3);
* [заполнение топика данными](#step4);
* [проверка содержимого таблицы](#step5).

## Шаг 1. Создание топика {#step1}

Сначала нужно создать [топик](../../concepts/datamodel/topic.md) в {{ ydb-short-name }}, из которого трансфер будет читать данные. Это можно сделать с помощью [SQL-запроса](../../yql/reference/syntax/create-topic.md):

```yql
CREATE TOPIC SourceTopic;
```

Этот топик `SourceTopic` позволяет передавать любые неструктурированные данные.

## Шаг 2. Создание таблицы {#step2}

После создания топика следует добавить [таблицу](../../concepts/datamodel/table.md), в которую будут поставляться данные из топика `SourceTopic`. Это можно сделать с помощью [SQL-запроса](../../yql/reference/syntax/create_table/index.md):

```yql
CREATE TABLE TargetTable (
  partition Uint32 NOT NULL,
  offset Uint64 NOT NULL,
  data String,
  PRIMARY KEY (partition, offset)
);
```

Эта таблица `TargetTable` имеет три столбца:

* `partition` — идентификатор [партиции](../../concepts/glossary.md#partition) топика, из которой получено сообщение;
* `offset` — [порядковый номер](../../concepts/glossary.md#offset), идентифицирующий сообщение внутри партиции;
* `data` — тело сообщения.

## Шаг 3. Создание трансфера {#step3}

После создания топика и таблицы следует добавить [трансфер](../../concepts/transfer.md) данных, который будет перекладывать сообщения из топика в таблицу. Это можно сделать с помощью [SQL-запроса](../../yql/reference/syntax/create-transfer.md):

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

CREATE TRANSFER Transfer
  FROM SourceTopic TO TargetTable
  USING $transformation_lambda;
```

В этом примере:

* `$transformation_lambda` - это правило преобразования сообщения из топика в колонки таблицы. В данном примере сообщение топика переносится в таблицу без преобразований;
* `$msg` - переменная, которая содержит обрабатываемое сообщение из топика.

## Шаг 4. Заполнение топика данными {#step4}

После создания трансфера можно записать в топик сообщение, например, используя [{{ ydb-short-name }} CLI](../../reference/ydb-cli/index.md).

{% include [x](../../_includes/ydb-cli-profile.md) %}

```bash
echo "Message 1" |  ydb --profile quickstart topic write SourceTopic
echo "Message 2" |  ydb --profile quickstart topic write SourceTopic
echo "Message 3" |  ydb --profile quickstart topic write SourceTopic
```

## Шаг 5. Проверка содержимого таблицы {#step5}

После записи сообщении в топик `SourceTopic` спустя некоторое время появятся записи в таблице `TargetTable`. Проверить их наличие можно с помощью [SQL-запроса](../../yql/reference/syntax/select/index.md):

```yql
SELECT *
FROM TargetTable;
```

Результат выполнения запроса:

```bash
partition offset  data
0         0       "Message 1"
0         1       "Message 2"
0         2       "Message 3"
```

Строки в таблицу добавляются не на каждое сообщение, полученное из топика, а предварительно батчуются. По умолчанию интервал записи в таблицу составляет 60 сек., либо объем записываемых данных должен достигнуть 8 Mb.

## Заключение

Данная статья приводит простой пример работы с трансфером: создание топика, таблицы и трансфера, записи в топик и проверки результата работы трансфера.

Более подробную информацию о трансфере см. [здесь](../../concepts/transfer.md).
