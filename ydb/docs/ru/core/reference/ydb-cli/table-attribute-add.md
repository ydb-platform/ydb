# table attribute add

С помощью команды `table attribute add` вы можете добавить [пользовательский атрибут](../../concepts/datamodel/table.md#users-attr) указанной таблице.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] table attribute add [options...] <table path>
```

* `global options` — [глобальные параметры](commands/global-options.md).
* `options` — [параметры подкоманды](#options).
* `table path` — путь таблицы.

Посмотрите описание команды добавления пользовательских атрибутов:

```bash
{{ ydb-cli }} table attribute add --help
```

## Параметры подкоманды {#options}

Имя | Описание
---|---
`--attribute` | Пользовательский атрибут в формате `<ключ>=<значение>`. Вы можете использовать `--attribute` несколько раз, чтобы добавить несколько атрибутов в одной команде.

## Примеры {examples}

Добавьте пользовательский атрибуты с ключами `attr_key1`, `attr_key2` и значениями `attr_value1`,`attr_value2` соответственно таблице `my-table`:

```bash
{{ ydb-cli }} table attribute add --attribute attr_key1=attr_value1 --attribute attr_key2=attr_value2 my-table
```
