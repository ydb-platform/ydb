--!syntax_pg
select user, current_user, current_role, current_catalog, current_schema, current_schema();
set search_path to "pg_catalog";
select user, current_user, current_role, current_catalog, current_schema, current_schema();


