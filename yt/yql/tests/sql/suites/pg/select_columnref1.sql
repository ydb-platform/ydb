--!syntax_pg
select key, value || '!', key || value as c from plato."Input";
