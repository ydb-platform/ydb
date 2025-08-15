## lambda-функция {#lambda}

[Lambda-функция](../syntax/expressions.md#lambda) преобразования сообщений принимает один параметр со структурой, содержащей сообщение из топика, и возвращает список структур, соответствующих строкам таблицы для вставки.

Пример:

```yql
$lambda = ($msg) -> {
  return [
    <|
      column_1: $msg._create_timestamp,
      column_2: $msg._data
    |>
  ];
};
```

В этом примере:

* `$msg` — сообщение, полученное из топика.
* `column_1` и `column_2` — названия колонок таблицы.
* `$msg._create_timestamp` и `$msg._data` — значения, которые будут записаны в таблицу. Типы значений должны совпадать с типами колонок таблицы. Например, если `column_2` имеет в таблице тип `String`, то и тип `$msg._data` должен быть именно `String`.

У сообщения топика доступны следующие поля:
  
| Атрибут             | Тип значения   | Описание                        |
|---------------------|----------------|----------------------------------|
| `_create_timestamp` | `Timestamp`    | Время создания сообщения        |
| `_data`             | `String`       | Тело сообщения                  |
| `_message_group_id` | `String`       | Идентификатор группы сообщений  |
| `_offset`           | `Uint64`       | [Смещение сообщения](../../../concepts/glossary.md#offset) |
| `_partition`        | `Uint32`       | Номер [партиции](../../../concepts/glossary.md#partition) сообщения |
| `_producer_id`      | `String`       | Идентификатор [писателя](../../../concepts/glossary.md#producer) |
| `_seq_no`           | `Uint64`       | Порядковый номер сообщения      |
| `_write_timestamp`  | `Timestamp`    | Время записи сообщения          |


### Тестирование lambda-функций

Для тестирования lambda-функции при её разработке можно в качестве сообщения топика передавать структуру с такими же полями, как будут передаваться в трансфере. Пример:

```yql
$lambda = ($msg) -> {
  return [
    <|
      offset: $msg._offset,
      data: $msg._data
    |>
  ];
};

$msg = <|
  _data: "value",
  _offset: CAST(1 AS Uint64),
  _partition: CAST(2 AS Uint32),
  _producer_id: "producer",
  _seq_no: CAST(3 AS Uint64)
|>;

SELECT $lambda($msg);
```

Если lambda-функция содержит сложную логику преобразования, то её можно выделить в отдельную lambda-функцию, что упростит тестирование.

```yql
$extract_value = ($data) -> {
  -- сложные преобразования
  return $data;
};

$lambda = ($msg) -> {
  return [
    <|
      column: $extract_value($msg._data)
    |>
  ];
};

-- Тестировать lambda-функцию extract_value можно так

SELECT $extract_value('преобразуемое значение');
```
