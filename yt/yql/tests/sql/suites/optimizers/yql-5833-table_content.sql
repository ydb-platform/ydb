/* postgres can not */
use plato;

$max_key = (
select
max(key)
from Input
);

select
cast(count(*) as String) || ' (' || cast($max_key as String) ||'/24)'
from Input
where key = $max_key;