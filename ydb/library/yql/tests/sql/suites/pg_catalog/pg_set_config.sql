--!syntax_pg
select set_config("search_path", "pg_catalog");
select oid, typinput::int4 as typinput, typname, typnamespace, typtype from pg_type;
select set_config("search_path", "public");
select oid, typinput::int4 as typinput, typname, typnamespace, typtype from pg_catalog.pg_type;