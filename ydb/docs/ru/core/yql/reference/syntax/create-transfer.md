# CREATE TRANSFER

Создает трансфер данных из [топика](../../../concepts/topic.md) в таблицу.

Синтаксис:

```yql
CREATE TRANSFER transfer_name 
FROM topic_name TO table_name USING lambda
WITH (option = value[, ...])
```

* `transfer_name` — имя трансфера. Может содержать строчные буквы латинского алфавита и цифры.
* `topic_name` — имя топика, содержащего исходные сообщения для последующего преобразования и записи в таблицу.
* `transfer_name` — имя таблицы, в которую будут записываться данные.
* `lambda` — lambda-функция преобразования сообщений.
* `option` — опция команды:
  * `CONNECTION_STRING` — [строка соединения](../../../concepts/connect.md#connection_string) c базой данной, содержащей топик. Указывается только если топик находится в другой базе {{ ydb-short-name }}.
  * Настройки для аутентификации в базе-источнике одним из способов (обязательно):

    * С помощью [токена](../../../recipes/ydb-sdk/auth-access-token.md):

      * `TOKEN_SECRET_NAME` — имя [секрета](../../../concepts/datamodel/secrets.md), содержащего токен.

    * С помощью [логина и пароля](../../../recipes/ydb-sdk/auth-static.md):

      * `USER` — имя пользователя.
      * `PASSWORD_SECRET_NAME` — имя [секрета](../../../concepts/datamodel/secrets.md), содержащего пароль.

  * `CONSUMER` — имя консьюмера топика источника. Если имя не задано, то консьюмер будет добавлен топику автоматически.

## Примеры {#examples}

{% note tip %}

Перед созданием трансфера [создайте](create-object-type-secret.md) секрет с аутентификационными данными для подключения.

{% endnote %}

Создание экземпляра трансфера из топика `example_topic` базы данных `/Root/another_database` в таблицу `example_table` текущей базы данных :

```yql
$transformation_lambda = ($msg) -> {
    return [
        <|
            partition:CAST($msg._partition AS Uint32),
            offset:CAST($msg._offset AS Uint32),
            message:CAST($msg._data AS Utf8)
        |>
    ];
};

CREATE TRANSFER example_transfer
    FROM example_topic TO example_table USING $transformation_lambda
WITH (
    CONNECTION_STRING = 'grpcs://example.com:2135/?database=/Root/another_database',
    TOKEN_SECRET_NAME = 'my_secret'
);
```

Создание экземпляра трансфера с явным указанием имени консьюмера `existing_consumer_of_topic`:

```yql
CREATE TRANSFER example_transfer
    FROM example_topic TO example_table USING $transformation_lambda
WITH (
    CONNECTION_STRING = 'grpcs://example.com:2135/?database=/Root/another_database',
    TOKEN_SECRET_NAME = 'my_secret',
    CONSUMER = 'existing_consumer_of_topic'
);
```



## См. также

* [ALTER TRANSFER](alter-transfer.md)
* [DROP TRANSFER](drop-transfer.md)
