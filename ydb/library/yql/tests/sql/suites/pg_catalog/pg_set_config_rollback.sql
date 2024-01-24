--!syntax_pg
select set_config('search_path', 'search_path', false);
commit;
select set_config('search_path', 'public', false);
rollback;
select oid, typinput::int4 as typinput, typname, typnamespace, typtype from pg_type;
