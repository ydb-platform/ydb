--!syntax_pg
select a.*, c.*, a.* from 
(select '1' x, 2 x) a,
(select 3 x, '4' x) c