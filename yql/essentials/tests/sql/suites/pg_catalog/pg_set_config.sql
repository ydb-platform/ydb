--!syntax_pg
select set_config('search_path', 'pg_catalog', false);
select oid, typinput::int4 as typinput, typname, typnamespace, typtype from pg_type order by oid;
select set_config('search_path', 'public', false);
select oid, typinput::int4 as typinput, typname, typnamespace, typtype from pg_catalog.pg_type order by oid;

