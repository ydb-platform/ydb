--!syntax_pg
select
count(*),
min(table_schema),
min(table_name),
min(column_name),
min(udt_name),
max(table_schema),
max(table_name),
max(column_name),
max(udt_name)
from
information_schema.columns

