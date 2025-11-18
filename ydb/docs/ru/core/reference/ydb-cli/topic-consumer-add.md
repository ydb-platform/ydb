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
`--starting-message-timestamp VAL` | Время в формате [UNIX timestamp](https://ru.wikipedia.org/wiki/Unix-время) (секунды с 1970.01.01) или в формате ISO-8601 (например, `2020-07-10T15:00:00Z`). Чтение начнется с первого [сообщения](../../concepts/datamodel/topic.md#message), полученного после указанного времени. Если время не задано, то чтение начнется с самого старого сообщения в топике.
`--supported-codecs` | Поддерживаемые методы сжатия данных.<br/>Значение по умолчанию — `raw`.<br/>Возможные значения:<ul><li>`RAW` — без сжатия;</li><li>`ZSTD` — сжатие [zstd](https://ru.wikipedia.org/wiki/Zstandard);</li><li>`GZIP` — сжатие [gzip](https://ru.wikipedia.org/wiki/Gzip);</li><li>`LZOP` — сжатие [lzop](https://ru.wikipedia.org/wiki/Lzop).</li></ul>
`--important` | Указывает, является ли читатель важным.<br/>Значение по умолчанию — `false`.<br/>Для важных читателей:<ul><li>не применяется ограничение по периоду доступности (`--availability-period`);</li><li>данные в топике не удаляются, пока они не прочитаны всеми важными читателями;</li><li>это влияет на процесс очистки данных в топике.</li></ul>Используйте этот параметр для критически важных читателей, которые должны гарантированно прочитать все сообщения.
`--availability-period VAL` | Время хранения данных в топике, для которых читатель ещё не потдвердил их обработку.<br/>Непрочитанные данные, возраст которых меньше указанного значения, не удаляются из топика.<br/>Формат: положительное число с указанием единицы измерения времени (без пробелов).<br/>Поддерживаются следующие единицы измерения:<ul><li>`s` — секунды (например, `30s`, `120s`);</li><li>`m` — минуты (например, `5m`, `1440m`);</li><li>`h` — часы (например, `1h`, `72h`);</li><li>`d` — дни (например, `1d`, `7d`).</li></ul>Примеры: `72h`, `1440m`, `2d`, `3600s`.<br/>Для важных читателей (с параметром `--important`) этот параметр не применяется.

## Примеры {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

**Создайте читателя с именем `my-consumer` для [созданного ранее](topic-create.md) топика `my-topic`, чтение начнется с первого сообщения, полученного после 15 августа 2022 13:00:00 GMT:**

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

**Создайте читателя с именем `backup-consumer` для топика `my-topic` с периодом доступности данных 3 дня:**

```bash
{{ ydb-cli }} -p quickstart topic consumer add \
  --consumer backup-consumer \
  --availability-period 3d \
  my-topic
```

В случае, если читатель успевает обрабатывать и подтверждать чтение, то данные в топике будут храниться в течение 2-х часов, в соответствии со значением параметра `retention-period` топика.
Однако при временной остановке чтения, время хранения данных в топике, для которых читатель `backup-consumer` ещё не подтвердил обработку, будет увеличено вплоть до 3-х дней, в соответствии с параметром `availability-period`.

**Создайте важного читателя с именем `critical-consumer` для топика `my-topic` с дополнительной поддержкой кодека `ZSTD`:**

```bash
{{ ydb-cli }} -p quickstart topic consumer add \
  --consumer critical-consumer \
  --important \
  --supported-codecs raw,gzip,zstd \
  my-topic
```

Обратите внимание, что для важного читателя параметр `--availability-period` не применяется, даже если он указан.
