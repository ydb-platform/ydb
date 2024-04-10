--!syntax_pg
select
datallowconn,
datdba,
datistemplate,
datname,
encoding,
oid,
datacl is null,
datcollate,
datctype
from
pg_catalog.pg_database
order by oid
