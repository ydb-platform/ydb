--!syntax_pg
select b.*,a.* from (select * from plato."Input") a, (select * from plato."Input2") b;
