pragma config.flags("PeepholeFlags","UseAggPhases");

$data = [<|x:1,y:0|>,<|x:1,y:0|>,<|x:2,y:1|>];

select y,min(x),sum(distinct x) from as_table($data) group by y order by y;
