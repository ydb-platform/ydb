/* syntax version 1 */
select
    avg(cast(key as int)) + 0.3 as key,
    cast(sum(cast(subkey as int)) as varchar) as subkey,
    value
from plato.Input
group by value
order by value;
