# Создание временных таблиц (TEMPORARY)

{% if backend_name == "YDB" and oss == true %}

{% include [OLAP_not_allow_note](../../../../_includes/not_allow_for_olap_note.md) %}

{% include [temp-table-description.md](../../../../_includes/temp-table-description.md) %}

{% endif %}

```sql
CREATE TEMPORARY TABLE table_name (
    ...
);
```