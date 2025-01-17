# Аутентификация Kafka API

## Когда аутентификация включена и когда выключена?

Аутентификация всегда включена при использовании [Kafka API в Yandex Cloud](https://yandex.cloud/ru/docs/data-streams/kafkaapi/auth)

Аутентификация по умолчанию выключена при использовании Docker образа.
Чтобы включить ее, укажите в yaml [конфигурации кластера](https://ydb.tech/docs/ru/reference/configuration/) параметр security.enforce_user_token=true

## Как работает аутентификация в Kafka API?

В Kafka API аутентификация выполняется через механизмы `SASL_PLAINTEXT/PLAIN` или `SASL_SSL/PLAIN`.

Для аутентификации необходимы:

* `<user-name>` — имя пользователя. Об управлении пользователями читайте в разделе [Управление доступом](../../security/access-management.md).
* `<password>` — пароль пользователя. Об управлении пользователями читайте в разделе [Управление доступом](../../security/access-management.md).
* `<database>` — [путь базы данных](../../concepts/connect#database).

Из этих параметров формируются:

* `<sasl.username>` = `<user-name>@<database>`
* `<sasl.password>` = `<password>`

{% note warning %}

Обратите внимание, логика формирования `<sasl.username>` и `<sasl.password>` в облачных инсталляциях {{ ydb-short-name }} может отличаться от приведенной здесь.

{% endnote %}

Примеры аутентификации смотрите в [Чтение и запись](./read-write.md).