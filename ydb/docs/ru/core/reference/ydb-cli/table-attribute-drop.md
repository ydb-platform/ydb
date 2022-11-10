# table attribute drop

С помощью команды `table attribute drop` вы можете удалить [пользовательский атрибут](../../concepts/datamodel/table.md#users-attr) указанной таблицы.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] table attribute drop [options...] <table path>
```

* `global options` — [глобальные параметры](commands/global-options.md).
* `options` — [параметры подкоманды](#options).
* `table path` — путь таблицы.

Посмотрите описание команды добавления пользовательских атрибутов:

```bash
{{ ydb-cli }} table attribute drop --help
```

## Параметры подкоманды {#options}

Имя | Описание
---|---
`--attributes` | Ключ пользовательского атрибута, который нужно удалить. Вы можете указать несколько ключей, используя `,` в качестве разделителя.

## Примеры {examples}

Удалите пользовательские атрибуты с ключами `attr_key1` и `attr_key2` таблицы `my-table`:

```bash
{{ ydb-cli }} table attribute drop --attributes attr_key1,attr_key2 my-table
```
