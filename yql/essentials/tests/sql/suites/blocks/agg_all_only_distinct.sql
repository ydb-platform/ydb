pragma config.flags("PeepholeFlags","UseAggPhases");

$data = [<|x:1|>,<|x:1|>,<|x:2|>];

select sum(distinct x) from as_table($data);
