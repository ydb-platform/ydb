/* syntax version 1 */
use plato;
pragma yt.JoinEnableStarJoin="true";
pragma DisablePullUpFlatMapOverJoin;

$a = select k1,v1,u1, 1 as t1 from Input1;
$c = select k3,v3,u3, 3 as t3 from Input3;

from any Input2 as b
join any $a as a on b.k2 = a.k1 and a.v1 = b.v2
join any $c as c on a.k1 = c.k3 and c.v3 = a.v1
select * order by u1

