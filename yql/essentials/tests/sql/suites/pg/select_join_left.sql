--!syntax_pg
select * from (select 1 as x ) u left join (select 1 as y) v on u.x=v.y;
select * from (select 1 as x ) u left join (select 2 as y) v on u.x=v.y;
