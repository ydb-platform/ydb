--!syntax_pg
select
count(*),
min(schemaname),
min(tablename),
max(schemaname),
max(tablename)
from pg_catalog.pg_tables;

