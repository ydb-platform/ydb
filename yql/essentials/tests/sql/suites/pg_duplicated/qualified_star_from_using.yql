--!syntax_pg
select a.*, b.* from 
(
    (select 1 x) a
    full join
    (select 2 x) b
    using(x)
)