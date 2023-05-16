# Сохранение позиции чтения

Каждый писатель топика обладает [позицией чтения](../../concepts/topic.md#consumer-offset).

С помощью команды `topic consumer offset commit` можно сохранить позицию чтения [добавленного ранее](topic-consumer-add.md) читателя.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] topic consumer offset commit [options...] <topic-path>
```

* `global options` — [глобальные параметры](commands/global-options.md).
* `options` — [параметры подкоманды](#options).
* `topic-path` — путь топика.

Посмотреть описание команды:

```bash
{{ ydb-cli }} topic consumer offset commit --help
```

## Параметры подкоманды {#options}

Имя | Описание
---|---
`--consumer <значение>` | Имя читателя.
`--partition <значение>` | Номер партиции.
`--offset <значение>` | Устанавливаемое значение смещения.

## Примеры {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

Установить для читателя с именем `my-consumer` смещение 123456789 в топике `my-topic` и партиции `1`:

```bash
{{ ydb-cli }} -p db1 topic consumer offset commit \
  --consumer my-consumer \
  --partition 1 \
  --offset 123456789 \
  my-topic
```
