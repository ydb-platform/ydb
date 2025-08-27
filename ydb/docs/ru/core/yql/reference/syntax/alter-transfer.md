# ALTER TRANSFER

Вызов `ALTER TRANSFER` изменяет параметры и состояние экземпляра [трансфера](../../../concepts/transfer.md).

## Синтаксис {#syntax}

```yql
ALTER TRANSFER <name> [SET USING lambda | SET (option = value [, ...])]
```

где:

* `name` — имя экземпляра трансфера.
* `lambda` — [lambda-функция](#lambda) преобразования сообщений.
* `SET (option = value [, ...])` — [параметры](#params) трансфера.

### Параметры {#params}

* `STATE` — [состояние](../../../concepts/transfer.md#pause-and-resume) трансфера. Возможные значения:
  * `PAUSED` — остановка трансфера.
  * `STANDBY` — возобновление работы трансфера после приостановки.

* {% include [x](../_includes/transfer_flush.md) %}


## Разрешения

Для изменения трансфера требуются [право](grant.md#permissions-list) изменять схемные объекты (`ALTER SCHEMA`).

## Примеры {#examples}

Следующий запрос изменяет [lambda-функцию](expressions.md#lambda) преобразования сообщений топика:

```yql
$new_lambda = ($msg) -> {
    return [
        <|
            partition: CAST($msg._partition AS Uint32),
            offset: CAST($msg._offset AS Uint64),
            message: CAST($msg._data || ' altered' AS Utf8)
        |>
    ];
};

ALTER TRANSFER my_transfer SET USING $new_lambda;
```

Следующий запрос временно приостанавливает работу трансфера:

```yql
ALTER TRANSFER my_transfer SET (STATE = "PAUSED");
```

Следующий запрос изменяет параметры батчевания:

```yql
ALTER TRANSFER my_transfer SET (
    BATCH_SIZE_BYTES = 1048576,
    FLUSH_INTERVAL = Interval('PT60S')
);
```

{% include [x](../_includes/transfer_lambda.md) %}

## См. также

* [CREATE TRANSFER](create-transfer.md)
* [DROP TRANSFER](drop-transfer.md)
* [{#T}](../../../concepts/transfer.md)
