# Сжатие данных в таблицах

{{ ydb-short-name }} позволяет сжимать данные таблиц на диске, чтобы уменьшить объём хранилища. Настройки сжатия задаются по-разному для [строковых](datamodel/table.md#row-oriented-tables) и [колоночных](datamodel/table.md#column-oriented-tables) таблиц. Синтаксис YQL описан в справочнике: [группы колонок](../yql/reference/syntax/create_table/family.md) для строковых таблиц и опция `COMPRESSION` для колонок в [CREATE TABLE](../yql/reference/syntax/create_table/index.md).

## Строковые таблицы {#row-oriented-tables}

В строковых таблицах сжатие настраивается на уровне [групп колонок](datamodel/table.md#column-groups). Чтобы включить сжатие для редко читаемых полей или вынести их на другой тип носителя, вы можете объединить колонки в отдельные группы.

Параметр `COMPRESSION` задаёт алгоритм сжатия для группы колонок. Допустимые значения:

- `off` — без сжатия;
- `lz4` — алгоритм [LZ4](https://ru.wikipedia.org/wiki/LZ4);
- `zstd` — алгоритм [Zstandard](https://ru.wikipedia.org/wiki/Zstandard) с настраиваемым уровнем сжатия.

Для `zstd` параметр `COMPRESSION_LEVEL` задаёт уровень сжатия — целое число от `0` до `22`. Значение `0` выбирает уровень по умолчанию самого Zstandard — `3`. Для `off` и `lz4` этот параметр не поддерживается.

Если вы не указали уровень сжатия:

- При [создании таблицы](../yql/reference/syntax/create_table/family.md) используется уровень по умолчанию, заданный администратором. Если он не задан, используется `1`.
- При [изменении группы колонок](../yql/reference/syntax/alter_table/family.md), которая уже использует ZSTD, сохраняется прежний уровень.
- При переключении с `off` или `lz4` на `zstd` используется `1`.

При переключении на `off` или `lz4` настройка `COMPRESSION_LEVEL` сбрасывается.

Колонки первичного ключа всегда входят в группу `default` и не переносятся в другие группы.

Изменения кодека сжатия и типа носителя не применяются немедленно к уже хранимым данным: новые параметры действуют при последующем фоновом [LSM compaction](glossary.md#compaction). Если вы меняете эти параметры в уже созданной таблице через `ALTER TABLE`, экономия места на диске тоже проявляется постепенно. Чтобы ускорить применение новых настроек к уже записанным данным, запустите [принудительный компакшен](../yql/reference/syntax/alter_table/compact.md) командой `ALTER TABLE … COMPACT`.

Создание и изменение групп колонок:

- [CREATE TABLE](../yql/reference/syntax/create_table/family.md)
- [ALTER TABLE](../yql/reference/syntax/alter_table/family.md)

## Колоночные таблицы {#column-oriented-tables}

В колоночных таблицах сжатие задаётся **отдельно для каждой колонки** опцией `COMPRESSION(algorithm=…[, level=…])` при [создании](../yql/reference/syntax/create_table/index.md) или [изменении](../yql/reference/syntax/alter_table/columns.md) таблицы.

Параметры:

- `algorithm` — `off`, `lz4` или `zstd`;
- `level` — уровень сжатия для алгоритма `zstd`; допустимые значения: от `0` до `22`. Для остальных алгоритмов параметр не используется.

Если опция `COMPRESSION()` указана без параметров, для колонки действует сжатие по умолчанию. Сейчас это `lz4`.

{% note info %}

В отличие от строковых таблиц, в колоночных смена настроек сжатия не гарантирует, что старые данные будут сжаты. Фоновый [компакшн](glossary.md#compaction) переписывает не всё — часть данных остаётся с прежними параметрами. Новые настройки применяются только к новым записям и к тем данным, которые компакшн обработает.

{% endnote %}

Колоночное хранение часто даёт высокую степень сжатия за счёт однородности данных в столбце. Вы также можете задать для колонок [кодирование данных](../yql/reference/syntax/create_table/index.md) отдельно от сжатия — опция `ENCODING`.

## Сравнение алгоритмов {#algorithm-comparison}

| Алгоритм | Свойства |
| -------- | -------- |
| `lz4` | Быстрый алгоритм с небольшими накладными расходами на CPU |
| `zstd` | Поддерживает настройку уровня сжатия; обычно даёт более высокую степень сжатия, чем `lz4` |
| `off` | Сжатие на стороне {{ ydb-short-name }} отключено |

## Примеры {#examples}

Строковая таблица: для часто читаемых колонок сжатие выключено, для большого JSON — `zstd` с уровнем `3` в отдельной группе. Доступность значений `DATA` зависит от конфигурации кластера — см. [CREATE TABLE — группы колонок](../yql/reference/syntax/create_table/family.md).

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
        COMPRESSION = "zstd",
        COMPRESSION_LEVEL = 3
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

Включение сжатия для группы `default`, например, если позже понадобилось сжать и часто читаемые колонки:

```yql
ALTER TABLE user_profiles ALTER FAMILY default SET COMPRESSION "lz4";
```

Изменение уровня сжатия ZSTD для группы `payload`:

```yql
ALTER TABLE user_profiles ALTER FAMILY payload SET COMPRESSION_LEVEL 5;
```

Перенос колонки `profile_json` в группу на носителе `rot`:

```yql
ALTER TABLE user_profiles
    ADD FAMILY cold (DATA = "rot", COMPRESSION = "lz4"),
    ALTER COLUMN profile_json SET FAMILY cold;
```

## Проверка настроек {#checking-settings}

Параметры сжатия входят в DDL таблицы. Их показывает YQL-команда [{#T}](../yql/reference/syntax/show_create.md) — для строковых таблиц в описании групп колонок (`FAMILY … COMPRESSION`), для колоночных — у каждой колонки (`COMPRESSION(...)`):

```yql
SHOW CREATE TABLE user_profiles;
```

В [{{ ydb-short-name }} CLI](../reference/ydb-cli/index.md) для строковой таблицы используйте команду [`scheme describe`](../reference/ydb-cli/commands/scheme-describe.md): в выводе есть блок `Column families` с полем `Compression`. Явно заданный уровень ZSTD отображается в скобках, например `ZSTD(5)`.

Вы также можете посмотреть схему таблицы в [{{ ydb-ui-name }}](../reference/ydb-ui/index.md).

## См. также {#see-also}

- [Таблицы](datamodel/table.md) — группы колонок, колоночное хранение.
- [{#T}](datamodel/topic.md#message-codec) — сжатие сообщений в топиках.
- [{#T}](query_execution/federated_query/s3/formats.md#compression) — сжатие файлов в S3 при федеративных запросах.
