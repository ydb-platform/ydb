# Команды работы со схемой топиков

YDB CLI поддерживает следующие команды для управления объектами схемы типа "топик":

* [topic create](#create) - Создание топика
* [topic alter](#alter) - Изменение параметров существующего топика
* [topic consumer add](#consumer-add) - Добавление читателя в топик
* [scheme describe](#describe) - Получение информации о параметрах топика
* [topic consumer drop](#consumer-drop) - Удаление читателя из топика
* [topic drop](#drop) - Удаление топика

В примерах используется профиль `db1`, подробнее смотрите в [{#T}](../../../getting_started/cli.md#profile).

## Создание топика {#create}

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] topic create <topic-path> [options...]
```

* `global options` — [глобальные параметры](../commands/global-options.md).
* `topic-path` — путь топика.
* `options` — [параметры](#create-options).

### Параметры {#create-options}

Имя | Описание
---|---
`--partitions-count VAL`| Количество [партиций](../../../concepts/topic.md#partitioning) топика.<br>Значение по умолчанию — `1`.
`--retention-period-hours VAL` | Время хранения данных в топике, задается в часах.<br>Значение по умолчанию — `18`.
`--supported-codecs STRING` | Поддерживаемые методы сжатия данных.<br>Значение по умолчанию — `raw,zstd,gzip,lzop`.<br>Возможные значения:<ul><li>`RAW` — без сжатия;</li><li>`ZSTD` — сжатие [zstd](https://ru.wikipedia.org/wiki/Zstandard);</li><li>`GZIP` — сжатие [gzip](https://ru.wikipedia.org/wiki/Gzip);</li><li>`LZOP` — сжатие [lzop](https://ru.wikipedia.org/wiki/Lzop).</li></ul>

### Примеры {#create-examples}

Создание топика с 2 партициями, методами сжатия `RAW` и `GZIP`, временем хранения сообщений 2 часа и путем `my-topic`:

```bash
{{ ydb-cli }} -p db1 topic create my-topic \
  --partitions-count 2 \
  --supported-codecs raw,gzip \
  --retention-period-hours 2
```

## Изменение топика {#alter}

С помощью подкоманды `topic alter` вы можете изменить [созданный ранее](#topic-create) топик.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] topic alter <topic-path> [options...]
```

* `global options` — [глобальные параметры](../commands/global-options.md).
* `topic-path` — путь топика.
* `options` — [параметры](#alter-options).

### Параметры {#alter-options}

При исполнении команды будут изменены значения тех параметров, которые заданы в командной строке. Значения остальных параметров останутся без изменений.

Имя | Описание
---|---
`--partitions-count VAL`| Количество [партиций](../../../concepts/topic.md#partitioning) топика. Возможно только увеличение количества партиций.
`--retention-period-hours VAL` | Время хранения данных в топике в часах.
`--supported-codecs STRING` | Поддерживаемые метод сжатия данных. <br>Возможные значения:<ul><li>`RAW` — без сжатия;</li><li>`ZSTD` — сжатие [zstd](https://ru.wikipedia.org/wiki/Zstandard);</li><li>`GZIP` — сжатие [gzip](https://ru.wikipedia.org/wiki/Gzip);</li><li>`LZOP` — сжатие [lzop](https://ru.wikipedia.org/wiki/Lzop).</li></ul>

### Примеры {#alter-examples}

Добавьте партицию и метод сжатия `lzop` [созданному ранее](#create-examples) топику:

```bash
{{ ydb-cli }} -p db1 topic alter my-topic \
  --partitions-count 3 \
  --supported-codecs raw,gzip,lzop
```

## Добавление читателя для топика {#consumer-add}

С помощью команды `topic consumer add` вы можете добавить читателя для [созданного ранее](#create) топика.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] topic consumer add <topic-path> [options...]
```

* `global options` — [глобальные параметры](../commands/global-options.md).
* `topic-path` — путь топика.
* `options` — [параметры подкоманды](#consumer-add-options).


### Параметры {#consumer-add-options}

Имя | Описание
---|---
`--consumer-name VAL` | Имя читателя, которого нужно добавить.
`--starting-message-timestamp VAL` | Время в формате [UNIX timestamp](https://ru.wikipedia.org/wiki/Unix-время). Чтение начнется с первого [сообщения](../../../concepts/topic.md#message), полученного после указанного времени. Если время не задано, то чтение начнется с самого старого сообщения в топике.

### Примеры {#consumer-add-examples}

Создайте читателя с именем `c1` для [созданного ранее](#create-examples) топика `my-topic`:

```bash
{{ ydb-cli }} -p db1 topic consumer add my-topic --consumer-name c1  
```

## Получение информации о параметрах топика {#describe}

Для получения информации о параметрах топика используется единая команда получения информации об объекте схемы [scheme describe](../commands/scheme-describe.md). Для топика данная команда выводит значения его параметров, а также перечень существующих читателей топика с их параметрами.

### Примеры {#describe-examples}

```bash
{{ ydb-cli }} -p db1 scheme describe my-topic
```

Результат:

```text
RetentionPeriod: 2 hours
PartitionsCount: 3
SupportedCodecs: RAW, GZIP, LZOP

Consumers: 
┌──────────────┬─────────────────┬───────────────────────────────┬───────────┐
| ConsumerName | SupportedCodecs | ReadFrom                      | Important |
├──────────────┼─────────────────┼───────────────────────────────┼───────────┤
| c1           | RAW, GZIP       | Mon, 15 Aug 2022 16:00:00 MSK | 0         |
└──────────────┴─────────────────┴───────────────────────────────┴───────────┘
```

## Удаление читателя {#consumer-drop}

С помощью команды `topic consumer drop` вы можете удалить [добавленного ранее](#consumer-add) читателя.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] topic consumer drop <topic-path> [options...] 
```

* `global options` — [глобальные параметры](../commands/global-options.md).
* `topic-path` — путь топика.
* `options` — [параметры подкоманды](#consumer-drop-options).

### Параметры {#consumer-drop-options}

Имя | Описание
---|---
`--consumer-name VAL` | Имя читателя, которого нужно удалить.

### Примеры {#consumer-drop-examples}

Удалите [ранее добавленного](#consumer-add) читателя с именем `c1` для топика `my-topic`:

```bash
{{ ydb-cli }} -p db1 topic consumer drop my-topic --consumer-name c1
```

## Удаление топика {#drop}

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] topic drop <topic-path>
```

* `global options` — [глобальные параметры](../commands/global-options.md).
* `topic-path` — путь топика.

### Примеры {#drop-examples}

```bash
{{ ydb-cli }} -p db1 topic drop my-topic
```

