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
* Настройки батчивания записи в таблицу:
  * `FLUSH_INTERVAL` - интервал записи в таблицу. Данные в таблицу будут записаны с указанной периодичностью.
  * `BATCH_SIZE_BYTES` - размер прочитанных из топика данных до записи данных в таблицу.

## Примеры {#examples}

Следующий запрос изменяет [lambda функцию](expressions.md#lambda) преобразовывающее сообщение топика:

```yql
$new_lambda = ...

ALTER TRANSFER my_transfer SET USING $new_lambda;
```

Следующий запрос временно приостанавливает работу трансфера:

```yql
ALTER TRANSFER my_transfer SET (STATE = "PAUSED");
```


## См. также

* [CREATE TRANSFER](create-transfer.md)
* [DROP TRANSFER](drop-transfer.md)
