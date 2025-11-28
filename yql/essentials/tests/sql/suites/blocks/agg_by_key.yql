pragma config.flags("PeepholeFlags","UseAggPhases");

$data = [<|x:1,y:0|>,<|x:3,y:0|>,<|x:2,y:1|>];

select y,sum(x) from as_table($data) group by y order by y;
