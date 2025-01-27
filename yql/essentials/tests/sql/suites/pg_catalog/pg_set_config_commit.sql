--!syntax_pg
select set_config('search_path', 'pg_catalog', false);
commit;
select oid, typinput::int4 as typinput, typname, typnamespace, typtype from pg_type;
rollback;
select oid, typinput::int4 as typinput, typname, typnamespace, typtype from pg_type;
