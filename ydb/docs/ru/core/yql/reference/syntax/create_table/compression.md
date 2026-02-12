# Сжатие данных колонок

{% if oss == true and backend_name == "YDB" %}

{% include [OLAP_only_allow_note](../../../../_includes/only_allow_for_olap_note.md) %}

{% endif %}

{% list tabs %}

- Создание колоночной таблицы

    ```yql
    CREATE TABLE compressed_table (
        id Uint64 NOT NULL,
        title Utf8 COMPRESSION(algorithm=lz4),
        info Utf8 COMPRESSION(algorithm=zstd, level=7),
        PRIMARY KEY (id)
    ) 
    WITH (STORE = COLUMN);
    ```

{% endlist %}

Для колонок можно задать следующие параметры сжатия:

* `algorithm` — алгоритм сжатия данных. Допустимые значения: `off`, `lz4`, `zstd`.

* `level` — уровень сжатия, поддерживается только для алгоритма `zstd` (допустимы значения от 0 до 22).
