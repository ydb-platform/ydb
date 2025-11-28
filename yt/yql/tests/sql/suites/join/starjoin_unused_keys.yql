/* syntax version 1 */
use plato;
pragma yt.JoinEnableStarJoin="true";

select 

v3

from any Input1 as a 
join any Input2 as b on (a.k1 = b.k2 and a.v1 = b.v2)
join any Input3 as c on (a.k1 = c.k3 and a.v1 = c.v3)
join Input4 as d on (a.k1 = d.k4)
order by v3;
