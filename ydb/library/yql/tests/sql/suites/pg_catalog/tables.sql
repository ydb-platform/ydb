--!syntax_pg
select
count(*) n,
min(table_schema) min_s,
min(table_name) min_t,
max(table_schema) max_s,
max(table_name) max_t
from information_schema.tables;

