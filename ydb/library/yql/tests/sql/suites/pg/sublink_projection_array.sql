--!syntax_pg
select array(select z from (values (1),(2)) a(z));
select array(select z from (values (array[1,2]),(array[3,4])) a(z));

