--!syntax_pg
select y,array(select x+u from (values (10),(20)) c(u)) from (values (1,2),(2,3)) a(x,y) order by y;
select y,array(select array[x+u,x+u+1] from (values (10),(20)) c(u)) from (values (1,2),(2,3)) a(x,y) order by y;

