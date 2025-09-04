--!syntax_pg
select * from (values (1),(1),(1)) a(x)
intersect all
select * from (values (1),(1)) a(x);

select * from (values (1),(1),(1)) a(x)
intersect all
select * from (values (2)) a(x);