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
`--partitions-count VAL`| Количество [партиций](../../concepts/topic.md#partitioning) топика. Возможно только увеличение количества партиций.
`--retention-period-hours VAL` | Время хранения данных в топике, задается в часах.
`--supported-codecs STRING` | Поддерживаемые методы сжатия данных. <br>Возможные значения:<ul><li>`RAW` — без сжатия;</li><li>`ZSTD` — сжатие [zstd](https://ru.wikipedia.org/wiki/Zstandard);</li><li>`GZIP` — сжатие [gzip](https://ru.wikipedia.org/wiki/Gzip);</li><li>`LZOP` — сжатие [lzop](https://ru.wikipedia.org/wiki/Lzop).</li></ul>

## Примеры {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

Добавьте партицию и метод сжатия `lzop` [созданному ранее](topic-create.md) топику:

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
