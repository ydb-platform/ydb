# Группы колонок

{% if oss == true and backend_name == "YDB" %}

{% include [OLTP_only_allow_note](../../../../_includes/only_allow_for_oltp_note.md) %}

{% endif %}

Колонки одной таблицы можно объединять в группы, чтобы задать следующие параметры:

* `DATA` — тип устройства хранения для данных колонок этой группы. Допустимые значения: `"ssd"`, `"rot"`.
* `COMPRESSION` — кодек сжатия данных. Допустимые значения: `"off"`, `"lz4"`.
* `CACHE_MODE` — [режим кэширования](../../../../concepts/datamodel/table.md#cache-modes). Допустимые значения: `"in_memory"`, `"regular"`.

По умолчанию все колонки находятся в одной группе с именем `default`. При необходимости параметры этой группы тоже можно переопределить. В противном случае применяются предопределённые значения.

В примере ниже для создаваемой таблицы добавляется группа колонок `family_large`, которая устанавливается для колонки `series_info`, а также переопределяются параметры для группы `default`, которая по умолчанию применяется ко всем остальным колонкам:

```sql
CREATE TABLE series_with_families (
    series_id Uint64,
    title Utf8,
    series_info Utf8 FAMILY family_large,
    release_date Uint64,
    PRIMARY KEY (series_id),
    FAMILY default (
        DATA = "ssd",
        COMPRESSION = "off",
        CACHE_MODE = "in_memory"
    ),
    FAMILY family_large (
        DATA = "rot",
        COMPRESSION = "lz4",
        CACHE_MODE = "regular"
    )
);
```

{% note info %}

Доступные типы устройств хранения зависят от конфигурации кластера {{ ydb-short-name }}.

{% endnote %}