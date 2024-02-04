--!syntax_pg
select 
oid,
relispartition,
relkind,
relname,
relnamespace,
relowner
from
pg_catalog.pg_class
order by oid

