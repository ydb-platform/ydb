# Выполнение запросов

{% include [./_includes/alert.md](./_includes/alert_preview.md) %}

Ранее запросы в диалекте PostgreSQL можно было выполнять двумя способами:

1. Через инструменты {{ ydb-short-name }} с маркером `--!syntax_pg` в начале тела запроса.
1. Через сетевой протокол PostgreSQL (pgwire) с помощью стандартных клиентов PostgreSQL.

**Оба способа удалены.** Запросы с маркером `--!syntax_pg` отклоняются, порт pgwire (5432) больше не используется.

Для выполнения запросов к {{ ydb-short-name }} используйте [YQL](../yql/reference/index.md) и стандартные инструменты {{ ydb-short-name }}:

```bash
ydb -e <ydb_address> -d <database_name> --user <user_name> sql -s 'SELECT 1;'
```

Где:

- `<ydb_address>` — адрес кластера {{ ydb-short-name }}, к которому выполняется подключение.
- `<database_name>` — название базы данных в кластере. Может быть сложным именем, например, `mycluster/tenant1/database/`.
- `<user_name>` — логин пользователя.
