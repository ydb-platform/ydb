# Аутентификация Kafka API

## Включение аутентификации

При [самостоятельном развертывании YDB](../../quickstart.md) по умолчанию используется [анонимная аутентификация](../../security/authentication.md#anonymous)
, не требующая логина-пароля.

Чтобы включить обязательную аутентификацию, следуйте инструкции в статье [Аутентификация](../../security/authentication.md#static-credentials).

Аутентификация всегда включена при использовании [Kafka API в Yandex Cloud](https://yandex.cloud/ru/docs/data-streams/kafkaapi/auth)

## Механизм аутентификации

В Kafka API аутентификация выполняется через механизмы `SASL_PLAINTEXT/PLAIN` или `SASL_SSL/PLAIN`.

Для аутентификации необходимы:

* `<user-name>` — имя пользователя. Об управлении пользователями читайте в разделе [{#T}](../../security/authentication.md).
* `<password>` — пароль пользователя. Об управлении пользователями читайте в разделе [{#T}](../../security/authentication.md).
* `<database>` — [путь базы данных](../../concepts/connect#database).

Из этих параметров формируются следующие переменные, которые вы можете использовать в
`sasl.jaas.config` параметре конфигурации клиента Kafka:

* `<sasl.username>` = `<user-name>@<database>`
* `<sasl.password>` = `<password>`

{% note warning %}

Обратите внимание, логика формирования `<sasl.username>` и `<sasl.password>` в облачных инсталляциях {{ ydb-short-name }} может отличаться от приведенной здесь.

{% endnote %}

Примеры аутентификации смотрите в [Чтение и запись](./examples.md).
