pragma config.flags("OptimizerFlags", "EqualityFilterOverJoin");

$a = select * from as_table([
	    <|x:Just(1), t:1, u:1, extra:1|>,
	    <|x:1, t:1, u:5, extra:2|>,
	]);

$b = select * from as_table([
	    <|y:1|>,
	    <|y:1|>,
	]);

$c = select * from as_table([
	    <|z:1|>,
	    <|z:1|>,
	]);

$d = select * from as_table([
	    <|c:2, d:3|>,
	    <|c:3, d:3|>,
	]);


select * from (
	select c.z as cz, b.y as by, a.u as au, a.t as at, a.x as ax, d.c as dc, d.d as dd from
	   $a as a right join $b as b on a.x=b.y
	   cross join $d as d
	   full join $c as c on b.y = c.z
)
where cz = at and by = au and ax = by and dc = dd;
