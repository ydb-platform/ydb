/* syntax version 1 */
use plato;
pragma yt.JoinEnableStarJoin="true";

from any Input2 as b
join any Input1 as a on b.k2 = a.k1 and a.v1 = b.v2
join any Input3 as c on a.k1 = c.k3 and c.v3 = a.v1
select * order by u1

