# ALTER SEQUENCE

`ALTER SEQUENCE` changes the parameters of an existing [sequence](../../../concepts/glossary.md#sequence) associated with a [serial column](../types/serial.md). Use it to change the increment or resume generation from a chosen value, for example, after loading data with explicit identifiers.

## Syntax

```yql
ALTER SEQUENCE [IF EXISTS] path_to_sequence
    [INCREMENT [BY] increment]
    [START [WITH] start_value]
    [RESTART [[WITH] restart_value]];
```

Specify at least one action: `INCREMENT`, `START`, or `RESTART`. Actions can appear in any order, without commas. Each action can appear only once per statement. Parameters you omit keep their values; if `RESTART` is absent, the generator position is not reset.

The `BY` and `WITH` keywords are optional and do not change the command's behavior. For example, `INCREMENT 5` is equivalent to `INCREMENT BY 5`, and `RESTART 1000` is equivalent to `RESTART WITH 1000`.

## Parameters

* `path_to_sequence`: the sequence object path. For a serial column, it has the form `<path_to_table>/_serial_column_<column_name>`, where `<path_to_table>` is the table path and `<column_name>` is the column name. For example, the `user_id` column of `/Root/users` uses `/Root/users/_serial_column_user_id`. For details on the object's location, see [{#T}](../../../concepts/datamodel/sequence.md).
* `IF EXISTS`: a missing sequence at the specified path is not an error. Without this clause, a statement targeting a missing object fails.
* `INCREMENT [BY] increment`: sets the increment for generated values. Specify `increment` as a positive integer literal; zero is not allowed. A new sequence has an increment of 1, but omitting `INCREMENT` from `ALTER SEQUENCE` preserves the current increment.
* `START [WITH] start_value`: stores the start value for subsequent `RESTART` actions without an argument. `START` alone does not change the next value to be allocated. A new sequence has a start value of 1.
* `RESTART [[WITH] restart_value]`: resets the generator position. With `restart_value`, generation resumes from that value. Without an argument, it uses the stored start value, including one set by `START` in the same statement. `RESTART` with an explicit argument does not change the stored start value.

Specify the start and restart values as integer literals. They are checked against the sequence's allowed range, and out-of-range values cause an error. Also account for the [serial column's value range](../types/serial.md#types-and-value-ranges).

{% note warning %}

`RESTART` does not remove or change table rows and does not search for an unused value. Resetting can produce values already in the table and cause conflicts when the entire primary key matches. For details, see [{#T}](../types/serial.md#explicit-values).

{% endnote %}

## Examples

The examples use the `users` table from [{#T}](../types/serial.md#usage-example), created in the `/Root` database. For another database or table, replace the sequence path with its corresponding absolute path.

### Changing the increment and generator position

Set the increment to 5 and resume generation from 1000:

```yql
ALTER SEQUENCE `/Root/users/_serial_column_user_id`
    INCREMENT BY 5
    RESTART 1000;
```

Subsequent requests to the generator allocate 1000, 1005, 1010, and so on. The stored start value does not change.

### Changing the start value

To make subsequent resets without an argument start at 1000, change the start value and then run `RESTART`:

```yql
ALTER SEQUENCE `/Root/users/_serial_column_user_id`
    INCREMENT BY 5
    START WITH 1000;
ALTER SEQUENCE `/Root/users/_serial_column_user_id` RESTART;
```

The first statement stores the start value and increment but does not reset the position. The second resumes generation from 1000. To store a new start value and reset the position immediately, specify `START WITH 1000` and `RESTART` in the same statement.
