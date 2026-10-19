# Сжатие данных в таблицах

{{ ydb-short-name }} сжимает данные таблиц на диске, чтобы уменьшить объём хранилища. Настройки сжатия задаются по-разному для [строковых](datamodel/table.md#row-oriented-tables) и [колоночных](datamodel/table.md#column-oriented-tables) таблиц. Синтаксис YQL описан в справочнике: [группы колонок](../yql/reference/syntax/create_table/family.md) и [параметры колонок](../yql/reference/syntax/_includes/column_option_list.md#compression).

## Строковые таблицы {#row-oriented-tables}

В строковых таблицах сжатие настраивается на уровне [групп колонок](datamodel/table.md#column-groups). Чтобы включить сжатие для редко читаемых полей или вынести их на другой тип носителя, вы можете объединить колонки в отдельные группы.

Для каждой группы колонок задаётся кодек сжатия `COMPRESSION`. Допустимые значения:

* `off` — без сжатия;
* `lz4` — алгоритм [LZ4](https://ru.wikipedia.org/wiki/LZ4); быстрый, с небольшими накладными расходами на CPU.

Для групп колонок строковых таблиц **не поддерживаются** другие алгоритмы, в том числе `zstd`. Колонки первичного ключа всегда входят в группу `default` и не переносятся в другие группы.

Создание и изменение групп: [CREATE TABLE — группы колонок](../yql/reference/syntax/create_table/family.md), [ALTER TABLE — группы колонок](../yql/reference/syntax/alter_table/family.md).

## Колоночные таблицы {#column-oriented-tables}

В колоночных таблицах сжатие задаётся **отдельно для каждой колонки** опцией `COMPRESSION(algorithm=…[, level=…])` при [создании](../yql/reference/syntax/create_table/index.md#compression) или [изменении](../yql/reference/syntax/alter_table/columns.md#compression) таблицы.

Параметры:

* `algorithm` — `off`, `lz4` или `zstd`;
* `level` — уровень сжатия для алгоритма `zstd`; допустимые значения: от `0` до `22`. Для остальных алгоритмов параметр не используется.

Если опция `COMPRESSION()` указана без параметров, для колонки действует сжатие по умолчанию. Сейчас это `lz4`.

Колоночное хранение часто даёт высокую степень сжатия за счёт однородности данных в столбце. Вы также можете задать для колонок [кодирование данных](../yql/reference/syntax/_includes/column_option_list.md#encoding) отдельно от сжатия — опция `ENCODING`.

## Когда применяются настройки {#when-settings-apply}

Изменения кодека сжатия и типа носителя для групп колонок **не переписывают уже записанные данные сразу**. Новые параметры применяются к данным в процессе фонового [LSM compaction](glossary.md#compaction).

После `ALTER TABLE`, меняющего сжатие, экономия места на диске проявится постепенно по мере compaction. На операции чтения и записи влияние выбранного алгоритма проявляется сразу для **новых** записей.

## Выбор алгоритма {#choosing-algorithm}

| Алгоритм | Где доступен | Когда уместен |
| -------- | ------------ | ------------- |
| `lz4` | Строковые и колоночные таблицы; в строковых — через группы колонок | Частые обновления, когда важны задержка и нагрузка на CPU |
| `zstd` | Только колоночные таблицы | Большие «холодные» колонки: логи, JSON, редко читаемые поля. Уровни `1`–`5` — баланс скорости и степени сжатия; более высокие уровни — если данные почти не перезаписываются |
| `off` | Обе модели | Данные уже сжаты на стороне приложения или сжатие не даёт выигрыша |

При интенсивной записи в колоночные таблицы вы можете начать с `lz4` или `zstd` с небольшим уровнем. Для архивных столбцов после оценки на репрезентативном объёме данных подойдёт более высокий уровень `zstd`.

## Примеры {#examples}

Строковая таблица: сжатие только через группы колонок и кодек `lz4`:

```yql
CREATE TABLE user_profiles (
    user_id Uint64,
    display_name Utf8,
    profile_json Utf8 FAMILY payload,
    PRIMARY KEY (user_id),
    FAMILY default (
        DATA = "ssd",
        COMPRESSION = "lz4"
    ),
    FAMILY payload (
        DATA = "ssd",
        COMPRESSION = "lz4"
    )
);
```

Колоночная таблица: разные алгоритмы для разных колонок:

```yql
CREATE TABLE analytics_events (
    event_id Uint64 NOT NULL,
    event_type Utf8,
    event_data Json COMPRESSION(algorithm="zstd", level=3),
    event_time Timestamp,
    PRIMARY KEY (event_id)
)
WITH (STORE = COLUMN);
```

Включение сжатия в существующей строковой таблице:

```yql
ALTER TABLE logs ALTER FAMILY default SET COMPRESSION "lz4";
```

Создание новой группы колонок и перенос колонки:

```yql
ALTER TABLE logs
    ADD FAMILY archive (DATA = "ssd", COMPRESSION = "lz4"),
    ALTER COLUMN message SET FAMILY archive;
```

## Проверка настроек {#checking-settings}

Текущие параметры групп колонок и опции колонок показывают команды:

```yql
DESCRIBE TABLE your_table;
```

```yql
SHOW CREATE TABLE your_table;
```

## См. также {#see-also}

* [Таблицы](datamodel/table.md) — группы колонок, колоночное хранение.
* [Пакетная загрузка данных](../dev/batch-upload.md) — рекомендуемый способ заливки в колоночные таблицы.
* [{#T}](datamodel/topic.md#message-codec) — сжатие сообщений в топиках.
* [{#T}](query_execution/federated_query/s3/formats.md#compression) — сжатие файлов в S3 при федеративных запросах.
