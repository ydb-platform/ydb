# ALTER TRANSFER

Вызов `ALTER TRANSFER` изменяет параметры и состояние экземпляра [трансфера](../../../concepts/transfer.md).

## Синтаксис {#syntax}

```yql
ALTER TRANSFER <name> [SET USING lambda] [SET (option = value [, ...])]
```

где:

* `name` — имя экземпляра трансфера.
* `SET (option = value [, ...])` — [параметры](#params) асинхронной репликации.

### Параметры {#params}

* `STATE` — состояние трансфера. Возможные значения:
  * `PAUSED` — временная остановка трансфера.
  * `STANDBY` — возмобновление работы трансфера после временной приостановки.
* Настройки батчивания записи в таблицу:
  * `FLUSH_INTERVAL` - интервал записи в таблицу. Данные в таблицу будет записаны в таблицу с указанной периодичностью.
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
