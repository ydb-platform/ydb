/* syntax version 1 */
/* postgres can not */
select x,count(*) as c from (
select AsList(1,2) as x 
union all
select AsList(1,3) as x 
union all
select AsList(1,2) as x 
) 
group by x
order by c;

select x,y,count(*) as c from (
select AsList(1,2) as x,AsList(4) as y 
union all
select AsList(1,3) as x,AsList(4) as y 
union all
select AsList(1,3) as x,AsList(4) as y 
union all
select AsList(1,3) as x,AsList(4) as y 
union all
select AsList(1,2) as x,AsList(5) as y 
union all
select AsList(1,2) as x,AsList(5) as y 
) group by x, y
order by c;
