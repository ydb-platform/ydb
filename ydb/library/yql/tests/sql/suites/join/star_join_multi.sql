/* syntax version 1 */
use plato;
pragma yt.JoinEnableStarJoin="true";


-- first Star JOIN chain 
$rightSemi = select * from Input2 as b right semi join Input1 as a on a.v1 = b.v2 and a.k1 = b.k2;
$leftOnly = select * from $rightSemi as rs left only join Input3 as c on rs.k1 = c.k3 and rs.v1 = c.v3;
$right = select * from Input4 as d right join $leftOnly as lo on d.v4 = lo.v1 and lo.k1 = d.k4;
$chain1 = select * from $right as r join any Input5 as e on r.k1 = e.k5 and e.v5 = r.v1;

-- second Star JOIN chain (mirror reflection of first one)
$leftSemi = select * from Input1 as a1 left semi join Input2 as b1 on b1.k2 = a1.k1 and a1.v1 = b1.v2;
$rightOnly = select * from Input3 as c1 right only join $leftSemi as ls on ls.k1 = c1.k3 and ls.v1 = c1.v3;
$left = select * from $rightOnly as ro left join Input4 as d1 on ro.v1 = d1.v4 and d1.k4 = ro.k1;
$chain2 = select * from any Input5 as e1 join $left as l on e1.k5 = l.k1 and l.v1 = e1.v5;


select left.k1 as k1, right.v1 as v1 from 
$chain1 as left join $chain2 as right on left.k1 = right.k1 and left.v1 = right.v1
order by k1,v1;

