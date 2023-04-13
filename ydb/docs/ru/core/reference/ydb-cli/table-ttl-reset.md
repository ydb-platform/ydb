# Сброс параметров TTL

С помощью подкоманды `table ttl reset` вы можете выключить [TTL](../../concepts/ttl.md) для указанной таблицы.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] table ttl reset [options...] <table path>
```

* `global options` — [глобальные параметры](commands/global-options.md).
* `options` — [параметры подкоманды](#options).
* `table path` — путь таблицы.

Посмотрите описание команды выключения TTL:

```bash
{{ ydb-cli }} table ttl reset --help
```

## Примеры {examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

Выключите TTL для таблицы `series`:

```bash
{{ ydb-cli }} -p quickstart table ttl reset \
  series
```
