--!syntax_pg
select count(*) n,min(encrypted::text) min_enc,min(gss_authenticated::text) min_auth from (
select 
encrypted,
gss_authenticated,
pid
from 
pg_catalog.pg_stat_gssapi
) a
