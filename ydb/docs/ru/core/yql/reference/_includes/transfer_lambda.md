## lambda-функция {#lambda}

[Lambda-функция](../syntax/expressions.md#lambda) преобразования сообщений принимает один параметр, содержащий сообщение топика, и возвращает список структур, соответствующих строкам таблицы.

Пример:

```yql
$lambda = ($msg) -> {
  return [
    <|
      column_1: value_1,
      column_2: value_2
    |>
  ];
};
```

В этом примере:

* `$msg` - сообщение полученное из топика.
* `column_1` и `column_2` - название колонок таблицы.
* `value_1` и `value_2` - значения, которые будут записаны в таблицу. Типы значении должен совпадать с типом колонок таблицы. Так, например, если `column_1` имеет в таблице тип `Utf8`, то и тип `value_1` должен быть `Utf8`.

У сообщения топика доступны следующие поля:
  
| Аттрибут            | Тип значения   |  Описание                      |
|---------------------|----------------|--------------------------------|
| `_create_timestamp` | `Timestamp`    | Время создания сообщения       |
| `_data`             | `String`       | Тело сообщения                 |
| `_message_group_id` | `String`       | Идентификатор группы сообщений |
| `_offset`           | `Uint64`       | [Смещение сообщения](../../../concepts/glossary.md#offset) |
| `_partition`        | `Uint32`       | Номер [партиции](../../../concepts/glossary.md#partition) сообщения |
| `_producer_id`      | `String`       | Идентификатор [писателя](../../../concepts/glossary.md#producer) |
| `_seq_no`           | `Uint64`       | Порядковый номер сообщения    |
| `_write_timestamp`  | `Timestamp`    | Время записи сообщения        |

Для тестирования lambda-функции можно в качестве сообщения топика передавать структуру с такими же полями.

Пример

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

Если lambda-функция содержит сложную логику преобразования, то ее можно выделить в отдельную lambda-функцию, что упростит тестирование.

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
