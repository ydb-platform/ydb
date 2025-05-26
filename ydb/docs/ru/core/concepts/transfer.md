# Трансфер данных

Трансфер в {{ ydb-short-name }} — это ассинхронный процесс переноса данных из [топика](topic.md) в таблицу, который упрощает процесс добавления и актуализации данных в таблице из внешних источников.

Часто при интеграции со внешними приложениями удобно писать данные не напрямую в таблицу, а в очередь сообщений, и уже в ассинхронном режиме перекладывать данные из топика в таблицу. Такой подход позволяет равномерно распределять нагрузку и переживать всплески нагрузки т.к. запись в очередь сообщений более легкая операция.

Реализация трансфера в {{ ydb-short-name }} читает сообщения из топика, преобразует их в пригодную для записи в таблицу форму, и записывает в таблицу. Преобразование данных происходит при помощи [lambda функции](../yql/reference/syntax/expressions.md#lambda), параметром которой является сообщение, а результатом является список [структур](../yql/reference/types/containers.md), каждая из которых соответствуюет добавляемой или модифицируемой строке таблицы. Например, если в таблице есть колонка с названием `example_column` типа Int32, то структура должна содержать именнованнное поле `example_column` типа Int32. Если [lambda функция](../yql/reference/syntax/expressions.md#lambda) возвращает пустой список, то в таблице не будет добавлено или модифицированно ни одной строки.

{% note info %}

При обновлении строки в таблице происходит ее полное перезатирание значениями из возвращаемой структуры. Если возвращаемая структура не содержит именованного поля соответствующего колонке таблицы, то значение колонки будет установлено в NULL. Именнованнные поля для которых нет соответствующей колонки в таблице будут проигнорированы.

{% endnote %}

{% note info %}

Рекомендуется что бы сообщения соответствующие одной строке таблицы находились в одной партиции топика т.к. в [топике](topic.md) определен порядок сообщений только в рамках одной партиции.

{% endnote %}

## Обработка ошибок в процессе трансфера {#error-handling}

В процессе трансфера возможно возникновение разных классов ошибок:

* **Временные сбои**. Например, транспортные ошибки, перегрузка системы и т.д. Запросы будут повторяться до успешного выполнения.
* **Критичные ошибки**. Например, ошибки прав доступа, ошибки схемы и т.д. Процесс репликации будет остановлен, и в [описании](../reference/ydb-cli/commands/scheme-describe.md) экземпляра репликации будет указан текст ошибки.

После устранения причины ошибки трансфер может быть возобновлен через [установку](../yql/reference/syntax/alter-transfer.md#params) статуса `STANDBY`.

## Пример {#example}

```
CREATE TABLE example_table (
    partition Uint32 NOT NULL,
    offset Uint64 NOT NULL,
    message Utf8,
    PRIMARY KEY (partition, offset)
);

CREATE TOPIC example_topic;

$transformation_lambda = ($msg) -> {
    return [
        <|
            partition:CAST($x._partition AS Uint32),
            offset:CAST($x._offset AS Uint32),
            message:CAST($x._data AS Utf8)
        |>
    ];
};

CREATE TRANSFER example_transfer
  FROM example_topic TO example_table USING $transformation_lambda;
```

У сообщения [топика](topic.md) доступны следующие поля:
* `_data` - тело сообщения
* `_message_group_id` - идентификатор группы сообщений
* `_offset` - смещение сообщения
* `_partition`- номер партиции сообщения
* `_producer_id` - идентификатор источника
* `_seq_no`- порядковые номера сообщений


Пример обработки сообщения в формате JSON

```
// example message:
// {
//   "update": {
//     "operation":"value_1"
//   },
//   "key": [
//     "id_1",
//     "2019-01-01T15:30:00.000000Z"
//   ]
// }

$transformation_lambda = ($msg) -> {
    $json = CAST($msg._data AS JSON);
    return [
        <|
            timestamp: DateTime::MakeDatetime(DateTime::ParseIso8601(CAST(Yson::ConvertToString($json.key[1]) AS Utf8))),
            object_id: CAST(Yson::ConvertToString($json.key[0]) AS Utf8),
            operation: CAST(Yson::ConvertToString($json.update.operation) AS Utf8)
        |>
    ];
};
```

## См. также

* [CREATE TRANSFER](../yql/reference/syntax/create-transfer.md)
* [ALTER TRANSFER](../yql/reference/syntax/alter-transfer.md)
* [DROP TRANSFER](../yql/reference/syntax/drop-transfer.md)
