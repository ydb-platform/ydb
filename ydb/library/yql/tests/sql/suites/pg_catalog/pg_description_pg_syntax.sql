--!syntax_pg
select count(*), min(objoid), min(classoid), max(objoid), max(classoid) from pg_catalog.pg_description;