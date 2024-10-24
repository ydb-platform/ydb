# Изменение топика

С помощью подкоманды `topic alter` вы можете изменить [созданный ранее](topic-create.md) топик.

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

## Параметры подкоманды {#options}

При исполнении команды будут изменены значения тех параметров, которые заданы в командной строке. Значения остальных параметров останутся без изменений.

Имя | Описание
---|---
`--partitions-count`| Количество [партиций](../../concepts/topic.md#partitioning) топика. Возможно только увеличение количества партиций.
`--retention-period-hours` | Время хранения данных в топике, задается в часах.
`--partition-write-speed-kbps` | Максимальная скорость записи в [партицию](../../concepts/topic.md#partitioning), задается в КБ/с.<br/>Значение по умолчанию — `1024`.
`--retention-storage-mb` | Максимальный объем хранения, задается в МБ. При достижении ограничения будут удаляться самые старые данные.<br/>Значение по умолчанию — `0` (ограничение не задано).
`--supported-codecs` | Поддерживаемые методы сжатия данных.<br/>Возможные значения:<ul><li>`RAW` — без сжатия;</li><li>`ZSTD` — сжатие [zstd](https://ru.wikipedia.org/wiki/Zstandard);</li><li>`GZIP` — сжатие [gzip](https://ru.wikipedia.org/wiki/Gzip);</li><li>`LZOP` — сжатие [lzop](https://ru.wikipedia.org/wiki/Lzop).</li></ul>
`--metering-mode` | Режим тарификации топика для serverless базы данных.<br/>Возможные значения:<ul><li>`request-units` — по фактическому использованию.</li><li>`reserved-capacity` — по выделенным ресурсам.</li></ul>

## Примеры {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

Добавьте партицию и метод сжатия `lzop` [созданному ранее](topic-create.md) топику:

```bash
{{ ydb-cli }} -p quickstart topic alter \
  --partitions-count 3 \
  --supported-codecs raw,gzip,lzop \
  my-topic
```

Убедитесь, что параметры топика изменились:

```bash
{{ ydb-cli }} -p quickstart scheme describe my-topic
```

Результат:

```text
RetentionPeriod: 2 hours
PartitionsCount: 3
SupportedCodecs: RAW, GZIP, LZOP
```
