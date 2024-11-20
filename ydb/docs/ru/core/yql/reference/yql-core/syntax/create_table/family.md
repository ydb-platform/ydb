# Группы колонок

Колонки одной таблицы можно объединять в группы, для того чтобы задать следующие параметры:

* `DATA` — тип устройства хранения для данных колонок этой группы (поддерживается только для [строковых](../../../../concepts/datamodel/table.md#row-oriented-tables) таблиц). Допустимые значения: `"ssd"`, `"rot"`.
* `COMPRESSION` — кодек сжатия данных. Допустимые значения: `"off"`, `"lz4"`, `"zstd"` (только для [колоночных](../../../../concepts/datamodel/table.md#column-oriented-tables) таблиц).
* `COMPRESSION_LEVEL` — уровень сжатия кодека (поддерживается только для [колоночных](../../../../concepts/datamodel/table.md#column-oriented-tables) таблиц), если кодек поддерживает уровень сжатия.

По умолчанию все колонки находятся в одной группе с именем `default`.  При желании, параметры этой группы тоже можно переопределить.

В примерах ниже для создаваемых таблиц добавляется группа колонок `family_large` и устанавливается для колонки `series_info`, а также переопределяются параметры для группы `default`, которая по умолчанию установлена для всех остальных колонок.


{% list tabs %}

- Создание строковой таблицы

    ```sql
    CREATE TABLE series_with_families (
        series_id Uint64,
        title Utf8,
        series_info Utf8 FAMILY family_large,
        release_date Uint64,
        PRIMARY KEY (series_id),
        FAMILY default (
            DATA = "ssd",
            COMPRESSION = "off"
        ),
        FAMILY family_large (
            DATA = "rot",
            COMPRESSION = "lz4"
        )
    );
    ```

- Создание колоночной таблицы

    ```sql
    CREATE TABLE series_with_families (
        series_id Uint64,
        title Utf8,
        series_info Utf8 FAMILY family_large,
        release_date Uint64,
        PRIMARY KEY (series_id),
        FAMILY default (
            COMPRESSION = "lz4"
        ),
        FAMILY family_large (
            COMPRESSION = "zstd",
            COMPRESSION_LEVEL = 5
        )
    ) 
    WITH (STORE = COLUMN);
    ```

{% endlist %}

{% note info %}

Доступные типы устройств хранения зависят от конфигурации кластера {{ ydb-short-name }}.

{% endnote %}