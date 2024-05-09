# Аутентификация Kafka API
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