--!syntax_pg
select count(*),min(encrypted::text),min(gss_authenticated::text) from (
select 
encrypted,
gss_authenticated,
pid
from 
pg_catalog.pg_stat_gssapi
) a
