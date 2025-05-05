use plato;

pragma config.flags('OptimizerFlags', 'MemberNthOverFlatMap');
pragma yt.MapJoinLimit="1m";


$t1 = select k1, v1 from Input1;
$t2 = select k2, v2, u2 as renamed from Input2;

select
     a.*,
     b.v2,
from $t1 as a left join any $t2 as b on a.k1 = b.k2;
