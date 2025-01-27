PRAGMA DisableSimpleColumns;
PRAGMA DisablePullUpFlatMapOverJoin;
use plato;
pragma yt.MapJoinLimit="1m";

from       (select k1, v1, Just(1) as u1 from Input1) as a
cross join (select k2, v2, Just(2) as u2 from Input2) as b
select *
order by a.k1,b.k2,a.v1,b.v2;
