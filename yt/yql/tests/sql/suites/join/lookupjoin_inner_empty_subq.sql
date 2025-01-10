PRAGMA DisableSimpleColumns;

use plato;
pragma yt.LookupJoinLimit="64k";
pragma yt.LookupJoinMaxRows="100";

select * from Input1 as a
inner join (select * from Input2 where k2 = "not_existent") as b on a.k1 = b.k2;

