--!syntax_pg
select * from (values (1),(2)) q(x) cross join (values (3),(4)) u(y) join (values (5),(6)) v(z) on v.z>u.y;
select * from (values (3),(4)) u(y) join (values (5),(6)) v(z) on v.z>u.y cross join (values (1),(2)) q(x);
select * from (values (1),(2)) q(x),(values (3),(4)) u(y) join (values (5),(6)) v(z) on v.z>u.y;
select * from (values (3),(4)) u(y) join (values (5),(6)) v(z) on v.z>u.y,(values (1),(2)) q(x);
