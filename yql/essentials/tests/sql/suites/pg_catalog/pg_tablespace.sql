--!syntax_pg
select
oid,
spcname
from
pg_catalog.pg_tablespace
order by oid
