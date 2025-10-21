/* syntax version 1 */
use plato;
pragma yt.JoinEnableStarJoin="true";


select *
from Input1 as a
join any Input2 as b on a.k1 = b.k2 AND a.v1 = b.v2
join any Input3 as c on a.k1 = c.k3 AND a.v1 = c.v3
left join Input4 as d on (c.v3, c.u3) = (d.v4, d.u4)
