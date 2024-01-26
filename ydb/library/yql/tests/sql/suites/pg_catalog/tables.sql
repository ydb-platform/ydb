--!syntax_pg
select
table_schema,
table_name
from information_schema.tables
order by table_schema, table_name;

