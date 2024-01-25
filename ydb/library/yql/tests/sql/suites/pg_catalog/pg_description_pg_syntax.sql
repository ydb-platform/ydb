--!syntax_pg
select count(*), min(objoid), min(classoid), max(objoid), max(classoid), min(description), max(description) from pg_catalog.pg_description;
 