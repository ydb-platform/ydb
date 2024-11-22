--!syntax_pg
select * from (values (1),(2)) q(x) cross join (values (3),(4)) u(y);
select * from (values (1),(2)) q(x),(values (3),(4)) u(y);