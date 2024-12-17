--!syntax_pg
select 
count(*) n,
min(oid) min_oid,
min(relispartition::text) min_isrel,
min(relkind) min_relkind,
min(relname) min_relname,
min(relnamespace) min_relns,
min(relowner) min_relowner,
max(oid) max_oid,
max(relispartition::text) max_isrel,
max(relkind) max_relkind,
max(relname) max_relname,
max(relnamespace) max_relns,
max(relowner) max_relowner,
min(relam) min_am,
max(relam) max_am
from
pg_catalog.pg_class;

