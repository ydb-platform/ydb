# Удаление таблицы

С помощью подкоманды `table drop` вы можете удалить указанную таблицу.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] table drop [options...] <table path>
```

* `global options` — [глобальные параметры](commands/global-options.md).
* `options` — [параметры подкоманды](#options).
* `table path` — путь таблицы.

Посмотрите описание команды удаления таблицы:

```bash
{{ ydb-cli }} table drop --help
```

## Параметры подкоманды {#options}

Имя | Описание
---|---
`--timeout` | Время, в течение которого должна быть выполнена операция на сервере.

## Примеры {examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

Удалите таблицу `series`:

```bash
{{ ydb-cli }} -p quickstart table drop series
```
