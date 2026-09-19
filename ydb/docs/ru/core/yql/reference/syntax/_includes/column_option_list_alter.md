### FAMILY <family_name> (настройка колонки)

{% if oss == true and backend_name == "YDB" %}

{% include [OLTP_only_allow_note](../../../../_includes/only_allow_for_oltp_note.md) %}

{% endif %}

Указание принадлежности данной колонки к указанной группе колонок. Подробнее в разделе [{#T}](../create_table/family.md).

### DEFAULT <default_value>

{% note warning %}

Опция `DEFAULT` поддерживается:

* Только для [строковых](../../../../concepts/datamodel/table.md#row-oriented-tables) таблиц.
* Только с литеральными значениями.

{% endnote %}

Позволяет задать значение по умолчанию для колонки. Если при вставке строки значение для данной колонки не указано, будет использовано указанное значение по умолчанию. Значение по умолчанию должно соответствовать типу данных колонки.

### NOT NULL

`DROP NOT NULL` снимает ограничение `NOT NULL` с неключевой колонки, снова разрешая значения `NULL`. Операция поддерживается как для [строковых](../../../../concepts/datamodel/table.md#row-oriented-tables), так и для [колоночных](../../../../concepts/datamodel/table.md#column-oriented-tables) таблиц.

```yql
ALTER TABLE table_name ALTER COLUMN column_name DROP NOT NULL;
```

### COMPRESSION([algorithm=<algorithm_name>[, level=<value>]]) {#compression}

{% if oss == true and backend_name == "YDB" %}

{% include [OLAP_only_allow_note](../../../../_includes/only_allow_for_olap_note.md) %}

{% endif %}

Для колонок можно задать следующие параметры сжатия:

* `algorithm` — алгоритм сжатия данных. Допустимые значения: `off` (отключение сжатия), `lz4`, `zstd`.
* `level` — уровень сжатия, поддерживается только для алгоритма `zstd` (допустимы значения от 0 до 22).

Если `COMPRESSION()` указан без параметров, для колонки используется сжатие по умолчанию. Сейчас это `lz4`; в будущих версиях появится возможность настраивать сжатие по умолчанию на уровне кластера или таблицы.
