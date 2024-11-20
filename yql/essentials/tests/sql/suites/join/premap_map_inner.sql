PRAGMA DisableSimpleColumns;
PRAGMA DisablePullUpFlatMapOverJoin;
use plato;
pragma yt.MapJoinLimit="1m";

from (select k1, v1 as u1 from Input1) as a
join (select k2, v2 as u2 from Input2) as b on a.k1 = b.k2
select a.k1,a.u1,b.u2
order by a.k1,a.u1;
