## Lambda function {#lambda}

A message transformation [lambda function](../syntax/expressions.md#lambda) takes a single structured parameter containing the message from the topic and returns a list of structures corresponding to the table rows for insertion.

Example:

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

In this example:

* `$msg` — the message received from the topic.
* `column_1` and `column_2` — the names of the table columns.
* `$msg._create_timestamp` and `$msg._data` — the values that will be written to the table. The value types must match the table column types. For example, if the `column_2` table column has the `String` type, the type of `$msg._data` must also be `String`.

The following fields are available in a topic message:

| Attribute           | Value type     | Description                      |
|---------------------|----------------|----------------------------------|
| `_create_timestamp` | `Timestamp`    | Message creation time            |
| `_data`             | `String`       | Message body                     |
| `_offset`           | `Uint64`       | [Message offset](../../../concepts/glossary.md#offset) |
| `_partition`        | `Uint32`       | Message's [partition](../../../concepts/glossary.md#partition) number |
| `_producer_id`      | `String`       | [Producer](../../../concepts/glossary.md#producer) ID|
| `_seq_no`           | `Uint64`       | Message sequence number         |
| `_write_timestamp`  | `Timestamp`    | Message write time              |


### Testing lambda functions

To test a lambda function during development, you can simulate a topic message by passing a structure with the same fields that the transfer will provide. Example:

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

If a lambda function contains complex transformation logic, you can extract it into a separate lambda function to simplify testing.

```yql
$extract_value = ($data) -> {
  -- complex transformations
  return $data;
};

$lambda = ($msg) -> {
  return [
    <|
      column: $extract_value($msg._data)
    |>
  ];
};

-- You can test the extract_value lambda function like this

SELECT $extract_value('converted value');
```
