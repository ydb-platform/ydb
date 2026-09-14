pragma config.flags('OptimizerFlags', 'EmitPruneKeys');

$a = select * from as_table([
	    <|x:1, t:1|>,
	    <|x:1, t:1|>,
	    <|x:1, t:2|>,
	    <|x:3, t:1|>,
	    <|x:3, t:4|>,
	    <|x:3, t:2|>,
	]);

$b = select * from as_table([
	    <|x:1, y:1|>,
	    <|x:1, y:2|>,
	    <|x:1, y:3|>,
	    <|x:1, y:3|>,
	    <|x:2, y:3|>,
	    <|x:2, y:4|>,
	]);

$distinct_a = (select distinct * from $a);
select a.x from any $distinct_a as a join any $b as b on a.x == b.y;
