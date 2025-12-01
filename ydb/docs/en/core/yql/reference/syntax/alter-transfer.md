# ALTER TRANSFER

The `ALTER TRANSFER` statement modifies the parameters and state of a [transfer](../../../concepts/transfer.md) instance.

## Syntax {#syntax}

```yql
ALTER TRANSFER <name> [SET USING lambda | SET (option = value [, ...])]
```

where:

* `name` — the name of the transfer instance.
* `lambda` — the [lambda-function](#lambda) for message transformation. <!-- markdownlint-disable-line MD051 -->
* `SET (option = value [, ...])` — the transfer [parameters](#params).

### Parameters {#params}

* `STATE` — the transfer [state](../../../concepts/transfer.md#pause-and-resume). Possible values:

* `PAUSED` — pauses the transfer.
* `ACTIVE` — resumes a paused transfer.

* {% include [x](../_includes/transfer_flush.md) %}

## Permissions

Modifying a transfer requires the `ALTER SCHEMA` [permissions](grant.md#permissions-list).

## Examples {#examples}

The following query modifies the message transformation [lambda-function](expressions.md#lambda):

```yql
$new_lambda = ($msg) -> {
    return [
        <|
            partition: $msg._partition,
            offset: $msg._offset,
            message: CAST($msg._data || ' altered' AS Utf8)
        |>
    ];
};

ALTER TRANSFER my_transfer SET USING $new_lambda;
```

The following query pauses the transfer:

```yql
ALTER TRANSFER my_transfer SET (STATE = "PAUSED");
```

The following query modifies the batching parameters:

```yql
ALTER TRANSFER my_transfer SET (
    BATCH_SIZE_BYTES = 1048576,
    FLUSH_INTERVAL = Interval('PT60S')
);
```

{% include [x](../_includes/transfer_lambda.md) %}

## See Also

* [CREATE TRANSFER](create-transfer.md)
* [DROP TRANSFER](drop-transfer.md)
* [{#T}](../../../concepts/transfer.md)
