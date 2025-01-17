/* syntax version 1 */
select
    key,
    avg(cast(subkey as Float)) as avg,
    sum(cast(subkey as Float)) as sum,
from plato.Input
group by key
order by key;
