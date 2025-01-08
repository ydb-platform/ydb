/* syntax version 1 */
use plato;
pragma yt.JoinEnableStarJoin="true";
pragma DisablePullUpFlatMapOverJoin;

$a = select k1,v1,u1, 1 as t1 from Input1;
$b = select k2,v2,u2, 2 as t2 from Input2;
$c = select k3,v3,u3, 3 as t3 from Input3;

from $a as a
left semi join $b as b on a.k1 = b.k2
left only join $c as c on a.k1 = c.k3
select * order by u1

