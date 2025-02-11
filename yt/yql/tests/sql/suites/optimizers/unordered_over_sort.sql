use plato;

pragma config.flags('OptimizerFlags', 'UnorderedOverSortImproved');

select * without key
from Input
where subkey in (select subkey from Dict where subkey > "0")

