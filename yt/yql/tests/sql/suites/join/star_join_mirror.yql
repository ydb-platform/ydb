/* syntax version 1 */
use plato;
pragma yt.JoinEnableStarJoin="true";

$leftSemi = select * from Input1 as a left semi join Input2 as b on b.k2 = a.k1 and a.v1 = b.v2;
$rightOnly = select * from Input3 as c right only join $leftSemi as ls on ls.k1 = c.k3 and ls.v1 = c.v3;
$left = select * from $rightOnly as ro left join Input4 as d on ro.v1 = d.v4 and d.k4 = ro.k1;
$inner = select * from any Input5 as e join $left as l on e.k5 = l.k1 and l.v1 = e.v5;

select * from $inner order by u1,u5;

