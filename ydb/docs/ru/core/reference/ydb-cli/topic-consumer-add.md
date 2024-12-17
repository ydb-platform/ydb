# Добавление читателя топика

С помощью команды `topic consumer add` вы можете добавить читателя для [созданного ранее](topic-create.md) топика.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] topic consumer add [options...] <topic-path>
```

* `global options` — [глобальные параметры](commands/global-options.md).
* `options` — [параметры подкоманды](#options).
* `topic-path` — путь топика.

Посмотрите описание команды добавления читателя:

```bash
{{ ydb-cli }} topic consumer add --help
```

## Параметры подкоманды {#options}

Имя | Описание
---|---
`--consumer VAL` | Имя читателя, которого нужно добавить.
`--starting-message-timestamp VAL` | Время в формате [UNIX timestamp](https://ru.wikipedia.org/wiki/Unix-время). Чтение начнется с первого [сообщения](../../concepts/topic.md#message), полученного после указанного времени. Если время не задано, то чтение начнется с самого старого сообщения в топике.
`--supported-codecs` | Поддерживаемые методы сжатия данных.<br/>Значение по умолчанию — `raw`.<br/>Возможные значения:<ul><li>`RAW` — без сжатия;</li><li>`ZSTD` — сжатие [zstd](https://ru.wikipedia.org/wiki/Zstandard);</li><li>`GZIP` — сжатие [gzip](https://ru.wikipedia.org/wiki/Gzip);</li><li>`LZOP` — сжатие [lzop](https://ru.wikipedia.org/wiki/Lzop).</li></ul>

## Примеры {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

Создайте читателя с именем `my-consumer` для [созданного ранее](topic-create.md) топика `my-topic`, чтение начнется с первого сообщения, полученного после 15 августа 2022 13:00:00 GMT:

```bash
{{ ydb-cli }} -p quickstart topic consumer add \
  --consumer my-consumer \
  --starting-message-timestamp 1660568400 \
  my-topic
```

Убедитесь, что читатель создан:

```bash
{{ ydb-cli }} -p quickstart scheme describe my-topic
```

Результат:

```text
RetentionPeriod: 2 hours
PartitionsCount: 2
SupportedCodecs: RAW, GZIP

Consumers:
┌──────────────┬─────────────────┬───────────────────────────────┬───────────┐
| ConsumerName | SupportedCodecs | ReadFrom                      | Important |
├──────────────┼─────────────────┼───────────────────────────────┼───────────┤
| my-consumer  | RAW, GZIP       | Mon, 15 Aug 2022 16:00:00 MSK | 0         |
└──────────────┴─────────────────┴───────────────────────────────┴───────────┘
```
