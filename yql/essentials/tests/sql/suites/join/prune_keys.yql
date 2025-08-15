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

$c = select * from as_table([
	    <|x:1|>,
	    <|x:1|>,
	    <|x:1|>,
	    <|x:1|>,
	    <|x:2|>,
	    <|x:2|>,
	]);

-- PruneKeys
select a.* from $a as a where a.x in (select x from $b); -- PruneKeys
select a.* from $a as a where a.x in (select /*+ distinct(x) */ x from $b); -- nothing
select a.* from $a as a where a.x in (select x from $c); -- PruneKeys
select a.* from $a as a left semi join $b as b on a.x = b.x; -- PruneKeys(b)
select a.* from $b as b right semi join $a as a on b.x = a.x; -- PruneKeys(b)
select a.x, a.t, b.x from any $a as a join $b as b on a.x == b.x; -- PruneKeys(a)
select a.x, a.t, b.x from $a as a join any $b as b on a.x == b.x; -- PruneKeys(b)

$a_sorted = select * from $a assume order by x;
$b_sorted = select * from $b assume order by x;
$c_sorted = select * from $c assume order by x;

-- PruneAdjacentKeys
select a.* from $a as a where a.x in (select x from $b_sorted); -- PruneAdjacentKeys
select a.* from $a as a where a.x in (select /*+ distinct(x) */ x from $b_sorted); -- nothing
select a.* from $a as a where a.x in (select x from $c_sorted); -- PruneAdjacentKeys
select a.* from $a as a left semi join $b_sorted as b on a.x = b.x; -- PruneAdjacentKeys(b_sorted)
select a.* from $b_sorted as b right semi join $a as a on b.x = a.x; -- PruneAdjacentKeys(b_sorted)
select a.x, a.t, b.x from any $a_sorted as a join $b as b on a.x == b.x; -- PruneAdjacentKeys(a_sorted)
select a.x, a.t, b.x from $a as a join any $b_sorted as b on a.x == b.x; -- PruneAdjacentKeys(b_sorted)
