# ALTER SEQUENCE

Изменяет параметры уже существующего объекта `Sequence`, привязанного к колонке [Serial](../types/serial.md) типа.

## Синтаксис

```yql
ALTER SEQUENCE [ IF EXISTS ] path_to_sequence
    [ INCREMENT [ BY ]     increment      ]
    [ START     [ WITH ]   start_value    ]
    [ RESTART   [ [ WITH ] restart_value ]];
```

## Параметры

* `path_to_sequence` - абсолютный путь до объекта `Sequence`.

    Путь формируется как `<path_to_table>/_serial_column_{column_name}`,
    где `<path_to_table>` — абсолютный путь до таблицы, a `{column_name}` — имя колонки типа `Serial`.
    Например, для таблицы с путём `/local/users` и колонки `user_id` путь к соответствующему `Sequence` будет `/local/users/_serial_column_user_id`.
* `IF EXISTS` - при использовании этой конструкции, выражение не возвращает ошибку, если не существует `Sequence` по указаному пути.
* `INCREMENT [ BY ] increment` - задает шаг изменения последовательности. Значение по умолчанию: 1.
* `START [ WITH ] start_value` - устанавливает новое стартовое значение для последовательности. Изменение этого параметра через `ALTER SEQUENCE` не влияет на текущее значение последовательности, но будет использовано, если выполнить `ALTER SEQUENCE RESTART` без указания значения.  Значение по умолчанию: 1.
* `RESTART [ [ WITH ] restart_value ]` - изменяет текущее значение последовательности на указанное в `restart_value`. Если значение не указано, текущее значение последовательности будет установлено в текущее стартовое значение.

## Примеры

``` yql
CREATE TABLE users (
    user_hash Uint64,
    user_id Serial,
    name Utf8,
    email Utf8,
    PRIMARY KEY (user_hash, user_id)
);
```

Изменить шаг последовательности для `user_id` и установить текущее значение равным 1000:

```yql
ALTER SEQUENCE `/Root/users/_serial_column_user_id`
    INCREMENT BY 5
    RESTART 1000;
```

Альтернативный способ изменить текущее значение — сначала изменить стартовое значение, а затем выполнить `RESTART` (после этого последующие сбросы через `RESTART` без указания `restart_value` будут устанавливать текущее значение в 1000):

```yql
ALTER SEQUENCE `/Root/users/_serial_column_user_id` INCREMENT BY 5 START WITH 1000;
ALTER SEQUENCE `/Root/users/_serial_column_user_id` RESTART;
```
