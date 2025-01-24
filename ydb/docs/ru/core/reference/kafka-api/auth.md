# Аутентификация Kafka API

## Включение аутентификации

Аутентификация всегда включена при использовании [Kafka API в Yandex Cloud](https://yandex.cloud/ru/docs/data-streams/kafkaapi/auth)

При самостоятельном развертывании YDB по умолчанию используется [анонимная аутентификация](../../security/authentication.md#anonymous)
, не требующая логина-пароля.
Чтобы включить обязательную аутентификацию, укажите опцию [`enforce_user_token_requirement` в конфиге](../configuration/index.md#auth).

## Механизм аутентификации

В Kafka API аутентификация выполняется через механизмы `SASL_PLAINTEXT/PLAIN` или `SASL_SSL/PLAIN`.

Для аутентификации необходимы:

* `<user-name>` — имя пользователя. Об управлении пользователями читайте в разделе [{#T}](../../security/authentication.md).
* `<password>` — пароль пользователя. Об управлении пользователями читайте в разделе [{#T}](../../security/authentication.md).
* `<database>` — [путь базы данных](../../concepts/connect#database).

Из этих параметров формируются:

* `<sasl.username>` = `<user-name>@<database>`
* `<sasl.password>` = `<password>`

{% note warning %}

Обратите внимание, логика формирования `<sasl.username>` и `<sasl.password>` в облачных инсталляциях {{ ydb-short-name }} может отличаться от приведенной здесь.

{% endnote %}

Примеры аутентификации смотрите в [Чтение и запись](./examples).
