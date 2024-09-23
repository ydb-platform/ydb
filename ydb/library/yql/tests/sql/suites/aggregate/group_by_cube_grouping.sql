/* syntax version 1 */
/* postgres can not */
select sum(length(value)) as s, m0, m1, m2, 2u * (2u * grouping(m0) + grouping(m1)) + grouping(m2) as ggg3
from plato.Input
group by cube(cast(key as uint32) as m0, cast(key as uint32) % 10u as m1, cast(key as uint32) % 100u as m2)
order by s, m0, m1, m2;
