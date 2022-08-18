# Работа с топиками

С помощью подкоманды `topic` вы можете создать, изменить или удалить [топик](../../concepts/topic.md), а также создать или удалить [читателя](../../concepts/topic.md#consumer).

В примерах используется профиль `db1`, подробнее смотрите в [{#T}](../../getting_started/cli.md#profile).

## Создание топика {#topic-create}

С помощью подкоманды `topic create` вы можете создать новый топик.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] topic create [options...] <topic-path>
```

* `global options` — [глобальные параметры](commands/global-options.md).
* `options` — [параметры подкоманды](#options).
* `topic-path` — путь топика.

Посмотрите описание команды создания топика:

```bash
{{ ydb-cli }} topic create --help
```

### Параметры подкоманды {#topic-create-options}

Имя | Описание
---|---
`--partitions-count VAL`| Количество [партиций](../../concepts/topic.md#partitioning) топика.<br>Значение по умолчанию — `1`.
`--retention-period-hours VAL` | Время хранения данных в топике, задается в часах.<br>Значение по умолчанию — `18`.
`--supported-codecs STRING` | Поддерживаемые метод сжатия данных.<br>Значение по умолчанию — `raw,zstd,gzip,lzop`.<br>Возможные значения:<ul><li>`RAW` — без сжатия;</li><li>`ZSTD` — сжатие [zstd](https://ru.wikipedia.org/wiki/Zstandard);</li><li>`GZIP` — сжатие [gzip](https://ru.wikipedia.org/wiki/Gzip);</li><li>`LZOP` — сжатие [lzop](https://ru.wikipedia.org/wiki/Lzop).</li></ul>

### Примеры {#topic-create-examples}

Создайте топик с 2 партициями, методами сжатия `RAW` и `GZIP`, временем хранения сообщений 2 часа и путем `my-topic`:

```bash
{{ ydb-cli }} -p db1 topic create \
  --partitions-count 2 \
  --supported-codecs raw,gzip \
  --retention-period-hours 2 \
  my-topic
```

Посмотрите параметры созданного топика:

```bash
{{ ydb-cli }} -p db1 scheme describe my-topic
```

Результат:

```text
RetentionPeriod: 2 hours
PartitionsCount: 2
SupportedCodecs: RAW, GZIP
```

## Изменение топика {#topic-alter}

С помощью подкоманды `topic alter` вы можете изменить [созданный ранее](#topic-create) топик.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] topic alter [options...] <topic-path>
```

* `global options` — [глобальные параметры](commands/global-options.md).
* `options` — [параметры подкоманды](#options).
* `topic-path` — путь топика.

Посмотрите описание команды изменения топика:

```bash
{{ ydb-cli }} topic alter --help
```

### Параметры подкоманды {#topic-alter-options}

Имя | Описание
---|---
`--partitions-count VAL`| Количество [партиций](../../concepts/topic.md#partitioning) топика.<br>Значение по умолчанию — `1`.
`--retention-period-hours VAL` | Время хранения данных в топике, задается в часах.<br>Значение по умолчанию — `18`.
`--supported-codecs STRING` | Поддерживаемые метод сжатия данных.<br>Значение по умолчанию — `raw,zstd,gzip,lzop`.<br>Возможные значения:<ul><li>`RAW` — без сжатия;</li><li>`ZSTD` — сжатие [zstd](https://ru.wikipedia.org/wiki/Zstandard);</li><li>`GZIP` — сжатие [gzip](https://ru.wikipedia.org/wiki/Gzip);</li><li>`LZOP` — сжатие [lzop](https://ru.wikipedia.org/wiki/Lzop).</li></ul>

### Примеры {#topic-alter-examples}

Добавьте партицию и метод сжатия `lzop` [созданному ранее](#topic-create) топику:

```bash
{{ ydb-cli }} -p db1 topic alter \
  --partitions-count 3 \
  --supported-codecs raw,gzip,lzop \
  my-topic
```

Убедитесь, что параметры топика изменились:

```bash
{{ ydb-cli }} -p db1 scheme describe my-topic
```

Результат:

```text
RetentionPeriod: 2 hours
PartitionsCount: 3
SupportedCodecs: RAW, GZIP, LZOP
```

## Удаление топика {#topic-drop}

С помощью подкоманды `topic drop` вы можете удалить [созданный ранее](#topic-create) топик.

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

### Примеры {#topic-drop-examples}

Удалите [созданный ранее](#topic-create) топик:

```bash
{{ ydb-cli }} -p db1 topic drop my-topic
```

## Создание читателя для топика {#consumer-add}

С помощью подкоманды `topic consumer add` вы можете создать читателя для [созданного ранее](#topic-create) топика.

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

### Параметры подкоманды {#consumer-add-options}

Имя | Описание
---|---
`--consumer-name VAL` | Имя читателя, которого нужно создать.
`--starting-message-timestamp VAL` | Время в формате [UNIX timestamp](https://ru.wikipedia.org/wiki/Unix-время). Чтение начнется с первого [сообщения](../../concepts/topic.md#message), полученного после указанного времени. Если время не задано, то чтение начнется с самой старого сообщения в топике.

### Примеры {#consumer-add-examples}

Создайте читателя с именем `my-consumer` для [созданного ранее](#topic-create) топика `my-topic`, чтение начнется с первого сообщения, полученного после 15 августа 2022 13:00:00 GMT:

```bash
{{ ydb-cli }} -p db1 topic consumer add \
  --consumer-name my-consumer \
  --starting-message-timestamp 1660568400 \
  my-topic 
```

Убедитесь, что читатель создан:

```bash
{{ ydb-cli }} -p db1 scheme describe my-topic
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

## Удаление читателя {#consumer-drop}

С помощью подкоманды `topic consumer drop` вы можете удалить [созданного ранее](#consumer-add) читателя.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] topic consumer drop [options...] <topic-path>
```

* `global options` — [глобальные параметры](commands/global-options.md).
* `options` — [параметры подкоманды](#options).
* `topic-path` — путь топика.

Посмотрите описание команды удаления читателя:

```bash
{{ ydb-cli }} topic consumer drop --help
```

### Параметры подкоманды {#consumer-drop-options}

Имя | Описание
---|---
`--consumer-name VAL` | Имя читателя, которого нужно удалить.

### Примеры {#consumer-drop-examples}

Удалите [ранее созданного](#consumer-add) читателя с именем `my-consumer` для топика `my-topic`:

```bash
{{ ydb-cli }} -p db1 topic consumer drop \
  --consumer-name my-consumer \
  my-topic
```
