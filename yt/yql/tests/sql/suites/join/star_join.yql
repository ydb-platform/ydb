/* syntax version 1 */
use plato;
pragma yt.JoinEnableStarJoin="true";


$rightSemi = select * from Input2 as b right semi join Input1 as a on a.v1 = b.v2 and a.k1 = b.k2;
$leftOnly = select * from $rightSemi as rs left only join Input3 as c on rs.k1 = c.k3 and rs.v1 = c.v3;
$right = select * from Input4 as d right join $leftOnly as lo on d.v4 = lo.v1 and lo.k1 = d.k4;
$inner = select * from $right as r join any Input5 as e on r.k1 = e.k5 and e.v5 = r.v1;

select * from $inner order by u1,u5;

