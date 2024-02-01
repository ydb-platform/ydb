--!syntax_pg
select
schemaname,
tablename
from pg_catalog.pg_tables
order by schemaname,tablename;

