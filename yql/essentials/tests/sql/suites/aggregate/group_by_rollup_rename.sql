/* syntax version 1 */
/* postgres can not */
$s = (
    select
        1 as x,
        2 as y
);

select
    x as x2,
    y
from $s
group by rollup(
    x, y
)
order by x2, y;
