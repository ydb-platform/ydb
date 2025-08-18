--!syntax_pg
select foo, q.foo as a1, w.foo as a2, e.foo as a3, r.foo as a4 from
    ((select 1 as foo) q
     full join
     (select 2 as foo) w
     using (foo))
    full join
    ((select 3 as foo) e
     full join
     (select 4 as foo) r
     using (foo))
    using(foo) order by foo