/* syntax version 1 */
/* postgres can not */
use plato;

$input = (select key, subkey, substring(value, 0, 1) == substring(value, 2, 1) as value_from_a from Input);

--insert into Output
select
    key,
    subkey,
    count_if(value_from_a) as approved,
    cast(count_if(value_from_a) as double) / count(*) as approved_share,
    count(*) as total
from $input
group by rollup(key, subkey)
order by key, subkey
;
