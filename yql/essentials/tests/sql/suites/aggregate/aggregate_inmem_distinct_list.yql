/* syntax version 1 */
/* postgres can not */
select listlength(aggregate_list(distinct x)) as c from (
select AsList(1,2) as x 
union all
select AsList(1,3) as x 
union all
select AsList(1,2) as x 
);

select count(distinct x) as c from (
select AsList(1,2) as x 
union all
select AsList(1,3) as x 
union all
select AsList(1,2) as x 
);

select x,count(distinct y) as c from (
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
) group by x
order by c;
