--!syntax_pg
select
table_schema, 
table_name, 
column_name,
udt_name
from
information_schema.columns
order by table_schema, table_name, column_name

