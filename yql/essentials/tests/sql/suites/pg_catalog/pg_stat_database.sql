--!syntax_pg
select
datid,
blks_hit,
blks_read,
tup_deleted
tup_fetched,
tup_inserted,
tup_returned,
tup_updated,
xact_commit,
xact_rollback
from pg_catalog.pg_stat_database
order by datid

