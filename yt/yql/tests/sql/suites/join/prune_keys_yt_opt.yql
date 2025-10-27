/* postgres can not */
use plato;

pragma config.flags('OptimizerFlags', 'EmitPruneKeys');

-- simple test
select count(*)
from Input1
where Input1.key in
    (select Input2.key 
    from Input2
    cross join Input3);

-- add PruneKeys in core opt and then in yt opt
select count(*)
from Input1
left semi join Input2
on Input1.key = Input2.key
right semi join Input3
on Input1.key = Input3.key;

-- add PruneKeys in yt opt multiple times
$part = (select Input1.key as key 
    from Input1
    cross join Input2);

select count(*)
from Input3
left semi join $part as part
on Input3.key = part.key
right semi join Input4
on Input3.key = Input4.key;

-- attempt to add PruneKeys in same child multiple times

$part = (select Input1.value as value 
    from Input1
    cross join Input2);

select count(*)
from Input3
right semi join $part as part
on Input3.value = part.value;

