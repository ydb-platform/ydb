--!syntax_pg
select
count(*),
min(table_schema),
min(table_name),
max(table_schema),
max(table_name)
from information_schema.tables;

