# Creation of temporary tables (TEMPORARY)

{% if oss == true and backend_name == "YDB" %}

{% include [OLAP_not_allow_note](../../../../_includes/not_allow_for_olap_note.md) %}

{% endif %}

{% include [temp-table-description.md](../../../../_includes/temp-table-description.md) %}

 ```sql
 CREATE TEMPORARY TABLE table_name (
     ...
 );
 ```