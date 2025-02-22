/* syntax version 1 */
use plato;
pragma yt.JoinEnableStarJoin="true";

from Input1 as a
left semi join Input2 as b on a.k1 = b.k2
left only join Input3 as c on a.k1 = c.k3
select * order by u1

