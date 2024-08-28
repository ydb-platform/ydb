{% include [x](../_includes/select/calc.md) %}

{% include [x](../_includes/select/execution.md) %}

{% include [x](../_includes/select/column_order.md) %}

{% include [x](../_includes/select/combining_queries.md) %}

{% include [x](../_includes/select/functional_tables.md) %}

## Поддерживаемые конструкции в SELECT

* [FROM](from.md)
* [FROM AS_TABLE](from_as_table.md)
* [FROM SELECT](from_select.md)
* [DISTINCT](distinct.md)
* [UNIQUE DISTINCT](unique_distinct_hints.md)
* [UNION](union.md)
* [WITH](with.md)
* [WITHOUT](without.md)
* [WHERE](where.md)
* [ORDER BY](order_by.md)
* [ASSUME ORDER BY](assume_order_by.md)
* [LIMIT OFFSET](limit_offset.md)
* [SAMPLE](sample.md)
* [TABLESAMPLE](sample.md)
{% if feature_map_reduce %}
* [FOLDER](folder.md)
* [WalkFolders](walk_folders.md)
* [VIEW](view.md)
{% endif %}
{% if feature_temp_table %}
* [TEMPORARY TABLE](temporary_table.md)
{% endif %}
{% if feature_bulk_tables %}
* [CONCAT](concat.md)
{% endif %}
{% if feature_secondary_index %}
* [VIEW INDEX](secondary_index.md)
{% endif %}