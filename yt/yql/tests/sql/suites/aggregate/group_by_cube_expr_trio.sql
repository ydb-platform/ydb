/* syntax version 1 */
/* postgres can not */
pragma sampleselect;

select sum(length(value)) as s, m0, m1, m2
from plato.Input
group by rollup(cast(key as uint32) as m0, cast(key as uint32) % 10u as m1, cast(key as uint32) % 100u as m2)
order by s, m0, m1, m2;
