select
oid,
relkind,
relname,
(select nspname from pg_catalog.pg_namespace as n where relnamespace = n.oid) as nspname
from
pg_catalog.pg_class
where relkind in ('r','v')
order by oid

