--!syntax_pg
select count(*),count(x),min(x),max(x),sum(x) from 
(values (1),(3),(null::int4)) as u(x)
