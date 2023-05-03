# Удаление топика

С помощью подкоманды `topic drop` вы можете удалить [созданный ранее](topic-create.md) топик.

{% note info %}

При удалении топика также будут удалены все добавленные для него читатели.

{% endnote %}

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] topic drop <topic-path>
```

* `global options` — [глобальные параметры](commands/global-options.md).
* `topic-path` — путь топика.

Посмотрите описание команды удаления топика:

```bash
{{ ydb-cli }} topic drop --help
```

## Примеры {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

Удалите [созданный ранее](topic-create.md) топик:

```bash
{{ ydb-cli }} -p quickstart topic drop my-topic
```
