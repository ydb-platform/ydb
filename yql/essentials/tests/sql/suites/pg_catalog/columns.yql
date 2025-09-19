--!syntax_pg
select
count(*) n,
min(table_schema) min_s,
min(table_name) min_t,
min(column_name) min_c,
min(udt_name) min_u,
max(table_schema) max_s,
max(table_name) max_t,
max(column_name) max_c,
max(udt_name) max_u
from
information_schema.columns

