# [Название действия]

[Краткое описание назначения команды. Если команда связана с другими, добавьте ссылки.]

[{% note info/warning %}

[Примечание, если необходимо]

{% endnote %}]

Общий вид команды:

    {{ ydb-cli }} [global options...] <command> [options...] [arguments...]

* `global options` — [глобальные параметры](commands/global-options.md).
* `options` — [параметры подкоманды](#options).
* `arguments` — [описание аргументов].

Посмотрите описание команды:

    {{ ydb-cli }} <command> --help

## Параметры подкоманды {#options}

[Если команда имеет параметры, добавьте таблицу или описание]

Имя | Описание
|---|---
`--parameter VAL` | Описание параметра.

## Примеры {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

[Пример 1]:

    {{ ydb-cli }} -p quickstart <command> [options...]

[Пример 2 с результатом]:

    {{ ydb-cli }} -p quickstart <command> [options...]

Результат:

    [Вывод команды]

[Ссылки на связанные команды или статьи, если необходимо]

