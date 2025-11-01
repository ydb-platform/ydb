PRAGMA DisableSimpleColumns;
PRAGMA DisablePullUpFlatMapOverJoin;
use plato;
pragma yt.MapJoinLimit="1m";

from (select k1, v1 || u1 as v1 from Input1) as a
left semi join (select k2, u2 || v2 as u2 from Input2) as b on a.k1 = b.k2
select *
order by a.k1,a.v1;
