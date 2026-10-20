# Сжатие данных в таблицах

{{ ydb-short-name }} позволяет сжимать данные таблиц на диске, чтобы уменьшить объём хранилища. Настройки сжатия задаются по-разному для [строковых](datamodel/table.md#row-oriented-tables) и [колоночных](datamodel/table.md#column-oriented-tables) таблиц. Синтаксис YQL описан в справочнике: [группы колонок](../yql/reference/syntax/create_table/family.md) и опция `COMPRESSION` в [CREATE TABLE](../yql/reference/syntax/create_table/index.md).

## Строковые таблицы {#row-oriented-tables}

В строковых таблицах сжатие настраивается на уровне [групп колонок](datamodel/table.md#column-groups). Чтобы включить сжатие для редко читаемых полей или вынести их на другой тип носителя, вы можете объединить колонки в отдельные группы.

Для каждой группы колонок задаётся кодек сжатия `COMPRESSION`. Допустимые значения:

* `off` — без сжатия;
* `lz4` — алгоритм [LZ4](https://ru.wikipedia.org/wiki/LZ4).

Для групп колонок строковых таблиц **не поддерживаются** другие алгоритмы, в том числе `zstd`. Колонки первичного ключа всегда входят в группу `default` и не переносятся в другие группы.

Изменения кодека сжатия и типа носителя не применяются немедленно к уже хранимым данным: новые параметры действуют при последующем фоновом [LSM compaction](glossary.md#compaction). Поэтому после `ALTER TABLE` экономия места на диске проявляется постепенно.

Создание и изменение групп: [CREATE TABLE — группы колонок](../yql/reference/syntax/create_table/family.md), [ALTER TABLE — группы колонок](../yql/reference/syntax/alter_table/family.md).

## Колоночные таблицы {#column-oriented-tables}

В колоночных таблицах сжатие задаётся **отдельно для каждой колонки** опцией `COMPRESSION(algorithm=…[, level=…])` при [создании](../yql/reference/syntax/create_table/index.md) или [изменении](../yql/reference/syntax/alter_table/columns.md) таблицы.

Параметры:

* `algorithm` — `off`, `lz4` или `zstd`;
* `level` — уровень сжатия для алгоритма `zstd`; допустимые значения: от `0` до `22`. Для остальных алгоритмов параметр не используется.

Если опция `COMPRESSION()` указана без параметров, для колонки действует сжатие по умолчанию. Сейчас это `lz4`.

Колоночное хранение часто даёт высокую степень сжатия за счёт однородности данных в столбце. Вы также можете задать для колонок [кодирование данных](../yql/reference/syntax/create_table/index.md) отдельно от сжатия — опция `ENCODING`.

## Сравнение алгоритмов {#algorithm-comparison}

| Алгоритм | Где доступен | Свойства |
| -------- | ------------ | -------- |
| `lz4` | Строковые и колоночные таблицы; в строковых — через группы колонок | Быстрый алгоритм с небольшими накладными расходами на CPU |
| `zstd` | Только колоночные таблицы | Поддерживает уровень сжатия `level`; обычно даёт более высокую степень сжатия, чем `lz4` |
| `off` | Строковые и колоночные таблицы | Сжатие на стороне {{ ydb-short-name }} отключено |

## Примеры {#examples}

Строковая таблица: для часто читаемых колонок сжатие выключено, для большого JSON — `lz4` в отдельной группе. Доступность значений `DATA` зависит от конфигурации кластера — см. [CREATE TABLE — группы колонок](../yql/reference/syntax/create_table/family.md).

```yql
CREATE TABLE user_profiles (
    user_id Uint64,
    display_name Utf8,
    profile_json Utf8 FAMILY payload,
    PRIMARY KEY (user_id),
    FAMILY default (
        DATA = "ssd",
        COMPRESSION = "off"
    ),
    FAMILY payload (
        DATA = "ssd",
        COMPRESSION = "lz4"
    )
);
```

Колоночная таблица с разными алгоритмами сжатия для разных колонок:

```yql
CREATE TABLE analytics_events (
    event_id Uint64 NOT NULL,
    event_type Utf8 COMPRESSION(algorithm=lz4),
    event_data Json COMPRESSION(algorithm=zstd, level=3),
    event_time Timestamp,
    PRIMARY KEY (event_id)
)
WITH (STORE = COLUMN);
```

Включение сжатия для группы `default`, например если позже понадобилось сжать и часто читаемые колонки:

```yql
ALTER TABLE user_profiles ALTER FAMILY default SET COMPRESSION "lz4";
```

Перенос колонки `profile_json` в группу на носителе `rot`:

```yql
ALTER TABLE user_profiles
    ADD FAMILY cold (DATA = "rot", COMPRESSION = "lz4"),
    ALTER COLUMN profile_json SET FAMILY cold;
```

## Проверка настроек {#checking-settings}

Параметры сжатия и групп колонок входят в DDL таблицы. Их показывает YQL-команда [{#T}](../yql/reference/syntax/show_create.md):

```yql
SHOW CREATE TABLE user_profiles;
```

В [{{ ydb-short-name }} CLI](../reference/ydb-cli/index.md) для строковой таблицы используйте команду [`scheme describe`](../reference/ydb-cli/commands/scheme-describe.md): в выводе есть блок `Column families` с полем `Compression`. Также вы можете посмотреть схему таблицы в [{{ ydb-ui-name }}](../reference/ydb-ui/index.md).

## См. также {#see-also}

* [Таблицы](datamodel/table.md) — группы колонок, колоночное хранение.
* [{#T}](datamodel/topic.md#message-codec) — сжатие сообщений в топиках.
* [{#T}](query_execution/federated_query/s3/formats.md#compression) — сжатие файлов в S3 при федеративных запросах.
