# Изменение сжатия данных колонок

{% if oss == true and backend_name == "YDB" %}

{% include [OLAP_only_allow_note](../../../../_includes/only_allow_for_olap_note.md) %}

{% endif %}

{% list tabs %}

- Изменение настроек сжатия колонки

    ```sql
    ALTER TABLE compressed_table ALTER COLUMN info SET COMPRESSION(algorithm=zstd, level=7);
    ```

- Сброс настроек сжатия колонки

    ```sql
    ALTER TABLE compressed_table ALTER COLUMN info SET COMPRESSION();
    ```

    После сброса колонка будет вести себя аналогично колонке, где сжатие не было задано.

{% endlist %}

Для колонок можно задать следующие параметры сжатия:

* `algorithm` — алгоритм сжатия данных. Допустимые значения: `off`, `lz4`, `zstd`.

* `level` — уровень сжатия, поддерживается только для алгоритма `zstd` (допустимы значения от 0 до 22).
