# Группы колонок

Колонки одной таблицы можно объединять в группы, чтобы задать следующие параметры:

* `DATA` — тип устройства хранения для данных колонок этой группы. Допустимые значения: `"ssd"`, `"rot"`.

{% if oss == true and backend_name == "YDB" %}

{% include [OLTP_only_allow_note](../../../../_includes/only_allow_for_oltp_note.md) %}

{% endif %}

* `COMPRESSION` — кодек сжатия данных. Допустимые значения: `"off"`, `"lz4"`, `"zstd"`.

{% if oss == true and backend_name == "YDB" %}

{% include [codec_zstd_allow_for_olap_note](../../../../_includes/codec_zstd_allow_for_olap_note.md) %}

{% endif %}

* `COMPRESSION_LEVEL` — уровень сжатия кодека, если кодек поддерживает уровень сжатия.

{% if oss == true and backend_name == "YDB" %}

{% include [OLAP_only_allow_note](../../../../_includes/only_allow_for_olap_note.md) %}

{% endif %}

По умолчанию все колонки находятся в одной группе с именем `default`. При необходимости параметры этой группы тоже можно переопределить. В противном случае применяются предопределённые значения.

В примерах ниже для создаваемых таблиц добавляется группа колонок `family_large`, которая устанавливается для колонки `series_info`, а также переопределяются параметры для группы `default`, которая по умолчанию применяется ко всем остальным колонкам.


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