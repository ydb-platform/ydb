--!syntax_pg
select key, key::oid in (select oid from pg_catalog.pg_type) as found from plato."Input" order by key
