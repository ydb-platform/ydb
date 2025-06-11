# ALTER TRANSFER

Вызов `ALTER TRANSFER` изменяет параметры и состояние экземпляра [трансфера](../../../concepts/transfer.md).

## Синтаксис {#syntax}

```yql
ALTER TRANSFER <name> [SET USING lambda] [SET (option = value [, ...])]
```

где:

* `name` — имя экземпляра трансфера.
* `lambda` — lambda функция преобразования сообщения.
* `SET (option = value [, ...])` — [параметры](#params) трансфера.

### Параметры {#params}

* `STATE` — состояние трансфера. Возможные значения:
  * `PAUSED` — остановка трансфера.
  * `STANDBY` — возобновление работы трансфера после приостановки.

## Примеры {#examples}

Следующий запрос изменяет [lambda-функцию](expressions.md#lambda) преобразования сообщений топика:

```yql
$new_lambda = ($msg) -> {
    return [
        <|
            partition:CAST($msg._partition AS Uint32),
            offset:CAST($msg._offset AS Uint32),
            message:CAST($msg._data || ' altered' AS Utf8)
        |>
    ];
};

ALTER TRANSFER my_transfer SET USING $new_lambda;
```

Следующий запрос временно приостанавливает работу трансфера:

```yql
ALTER TRANSFER my_transfer SET (STATE = "PAUSED");
```


## См. также

* [CREATE TRANSFER](create-transfer.md)
* [DROP TRANSFER](drop-transfer.md)
