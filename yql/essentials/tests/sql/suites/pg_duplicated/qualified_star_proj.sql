--!syntax_pg
select a.*, c.* from 
(select 1 x, 2 x) a,
(select 3 x, 4 x) c