# Удаление параметров TTL

С помощью подкоманды `table ttl drop` вы можете удалить [TTL](../../concepts/ttl.md) для указанной таблицы.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] table ttl drop [options...] <table path>
```

* `global options` — [глобальные параметры](commands/global-options.md).
* `options` — [параметры подкоманды](#options).
* `table path` — путь таблицы.

Посмотрите описание команды удаления TTL:

```bash
{{ ydb-cli }} table ttl drop --help
```

## Примеры {examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

Удалите TTL для таблицы `series`:

```bash
{{ ydb-cli }} -p db1 table ttl drop \
  series
```
