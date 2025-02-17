select
column_name,
table_name,
table_schema,
udt_name
from
information_schema.columns
order by table_schema, table_name, column_name
