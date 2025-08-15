--!syntax_pg
select string_agg(distinct x,y) from (values ('a',','),('b',':'),('a',',')) a(x,y);
select count(distinct x) from (values (1),(2),(1),(3),(2),(1)) a(x);