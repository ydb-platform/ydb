# Создание топика

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

## Параметры подкоманды {#options}

Имя | Описание
---|---
`--partitions-count`| Количество [партиций](../../concepts/topic.md#partitioning) топика.<br/>Значение по умолчанию — `1`.
`--retention-period-hours` | Время хранения данных в топике, задается в часах.<br/>Значение по умолчанию — `18`.
`--partition-write-speed-kbps` | Максимальная скорость записи в [партицию](../../concepts/topic.md#partitioning), задается в КБ/с.<br/>Значение по умолчанию — `1024`.
`--retention-storage-mb` | Максимальный объем хранения, задается в МБ. При достижении ограничения будут удаляться самые старые данные.<br/>Значение по умолчанию — `0` (ограничение не задано).
`--supported-codecs` | Поддерживаемые методы сжатия данных. Задаются через запятую.<br/>Значение по умолчанию — `raw`.<br/>Возможные значения:<ul><li>`RAW` — без сжатия;</li><li>`ZSTD` — сжатие [zstd](https://ru.wikipedia.org/wiki/Zstandard);</li><li>`GZIP` — сжатие [gzip](https://ru.wikipedia.org/wiki/Gzip);</li><li>`LZOP` — сжатие [lzop](https://ru.wikipedia.org/wiki/Lzop).</li></ul>
`--metering-mode` | Режим тарификации топика для serverless базы данных.<br/>Возможные значения:<ul><li>`request-units` — по фактическому использованию.</li><li>`reserved-capacity` — по выделенным ресурсам.</li></ul>

## Примеры {examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

Создание топика с 2 партициями, методами сжатия `RAW` и `GZIP`, временем хранения сообщений 2 часа и путем `my-topic`:

```bash
{{ ydb-cli }} -p quickstart topic create \
  --partitions-count 2 \
  --supported-codecs raw,gzip \
  --retention-period-hours 2 \
  my-topic
```

Посмотрите параметры созданного топика:

```bash
{{ ydb-cli }} -p quickstart scheme describe my-topic
```

Результат:

```text
RetentionPeriod: 2 hours
PartitionsCount: 2
SupportedCodecs: RAW, GZIP
```
