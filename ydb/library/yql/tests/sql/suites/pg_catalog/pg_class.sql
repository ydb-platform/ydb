--!syntax_pg
select 
count(*),
min(oid),
min(relispartition::text),
min(relkind),
min(relname),
min(relnamespace),
min(relowner),
max(oid),
max(relispartition::text),
max(relkind),
max(relname),
max(relnamespace),
max(relowner)
from
pg_catalog.pg_class;

