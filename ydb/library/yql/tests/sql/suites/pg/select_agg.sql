--!syntax_pg
select count(*) c1,count(x) c2,min(x) i1,max(x) a1,sum(x) s1 from 
(values (1),(3),(null::int4)) as u(x)
