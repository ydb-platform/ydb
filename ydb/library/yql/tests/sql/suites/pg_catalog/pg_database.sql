--!syntax_pg
select
datallowconn,
datdba,
datistemplate,
datname,
encoding,
oid
from
pg_catalog.pg_database
order by oid
