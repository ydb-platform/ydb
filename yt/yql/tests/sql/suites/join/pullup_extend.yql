pragma EmitUnionMerge;
pragma config.flags("OptimizerFlags", "PullUpExtendOverEquiJoin");
pragma yt.JoinMergeTablesLimit="10";

use plato;

$t1 = select k1 as k2, "111" as v2 from Input1;
$t2 = select k2, v2 from Input2;

$a = select * from $t1 
     union all
     select * from $t2
;

select
     a.k2 as k,
     a.v2 as av2
from $a as a join Input4 as b on a.k2 = b.k4;
