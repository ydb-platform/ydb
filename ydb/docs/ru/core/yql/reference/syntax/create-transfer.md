# CREATE TRANSFER

Создает трансфер данных из [топика](../../../concepts/topic.md) в таблицу.

Синтаксис:

```yql
CREATE TRANSFER transfer_name 
FROM topic_name TO table_name USING lambda
WITH (option = value[, ...])
```

* `transfer_name` — имя трансфера. Может содержать строчные буквы латинского алфавита и цифры.
* `topic_name` — имя топика из которого будут перекладываться в таблицу.
* `transfer_name` — имя таблицы в которую будут записываться данные.
* `option` — опция команды:
  * `CONNECTION_STRING` — [строка соединения](../../../concepts/connect.md#connection_string) c базой-источником. Обязательный параметр.
  * Настройки для аутентификации в базе-источнике одним из способов (обязательно):

    * С помощью [токена](../../../recipes/ydb-sdk/auth-access-token.md):

      * `TOKEN_SECRET_NAME` — имя [секрета](../../../concepts/datamodel/secrets.md), содержащего токен.

    * С помощью [логина и пароля](../../../recipes/ydb-sdk/auth-static.md):

      * `USER` — имя пользователя.
      * `PASSWORD_SECRET_NAME` — имя [секрета](../../../concepts/datamodel/secrets.md), содержащего пароль.

  * `CONSUMER` — имя консьюмера топика источника. Если имя не задано, то консьюмер будет добавлен топику автоматически.

  * Настройки батчивания записи в таблицу:
    * `FLUSH_INTERVAL` - интервал записи в таблицу. Данные в таблицу будет записаны в таблицу с указанной периодичностью.
    * `BATCH_SIZE_BYTES` - размер прочитанных из топика данных до записи данных в таблицу.

## См. также

* [ALTER TRANSFER](alter-transfer.md)
* [DROP TRANSFER](drop-transfer.md)
