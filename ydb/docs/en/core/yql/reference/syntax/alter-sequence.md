# ALTER SEQUENCE

Modifies the parameters of an existing `Sequence` object associated with a [Serial](../types/serial.md) column.

## Syntax

```yql
ALTER SEQUENCE [ IF EXISTS ] path_to_sequence
    [ INCREMENT [ BY ]     increment      ]
    [ START     [ WITH ]   start_value    ]
    [ RESTART   [ [ WITH ] restart_value ]];
```

## Parameters

* `path_to_sequence` — the absolute path to the `Sequence` object.

    The path is constructed as `<path_to_table>/_serial_column_{column_name}`,
    where `<path_to_table>` is the absolute path to the table, and `{column_name}` is the name of the `Serial` column.
    For example, for a table at `/local/users` and a column `user_id`, the corresponding `Sequence` path will be `/local/users/_serial_column_user_id`.

* `IF EXISTS` — if used, the statement does not return an error if the `Sequence` does not exist at the specified path.

* `INCREMENT [ BY ] increment` — sets the increment step for the sequence. Default: 1.

* `START [ WITH ] start_value` — sets a new start value for the sequence. Changing this parameter with `ALTER SEQUENCE` does not affect the current value, but it will be used with `ALTER SEQUENCE RESTART` if no value is specified. Default: 1.

* `RESTART [ [ WITH ] restart_value ]` — sets the current value of the sequence to the specified `restart_value`. If the value is not specified, the current value will be set to the current start value.

## Examples

```yql
CREATE TABLE users (
    user_hash Uint64,
    user_id Serial,
    name Utf8,
    email Utf8,
    PRIMARY KEY (user_hash, user_id)
);
```

Change the increment step for `user_id` and set the current value to 1000:

```yql
ALTER SEQUENCE `/Root/users/_serial_column_user_id`
    INCREMENT BY 5
    RESTART 1000;
```

An alternative way to change the current value is to first set a new start value, and then `RESTART` the `Sequence`. After this, subsequent calls to `RESTART` without an explicit value will set the current value to 1000:

```yql
ALTER SEQUENCE `/Root/users/_serial_column_user_id` INCREMENT BY 5 START WITH 1000;
ALTER SEQUENCE `/Root/users/_serial_column_user_id` RESTART;
```