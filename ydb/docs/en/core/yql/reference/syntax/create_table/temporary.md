# Creation of temporary tables (TEMPORARY)

{% if oss == true and backend_name == "YDB" %}

{% note warning %}

Not available at current time.
Can only be used for [`CREATE TABLE AS`](../as_select.md) execution.

{% endnote %}

{% endif %}

{% include [temp-table-description.md](../../../../_includes/temp-table-description.md) %}

```yql
 CREATE TEMPORARY TABLE table_name (
     ...
 );
 ```
