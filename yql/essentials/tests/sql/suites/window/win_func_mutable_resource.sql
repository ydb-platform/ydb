/* postgres can not */
select median(x) over w,median(x) over w from (
select x, 0 as y from (select AsList(1,2,3,4,5,6,7,8,9,10) as x) flatten by x
)
window w as (order by y)
