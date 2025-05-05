pragma config.flags('OptimizerFlags',
                    'MemberNthOverFlatMap',
                    'ExtractMembersSplitOnOptional',
                    'FilterNullMembersOverJust');

use plato;

$t1 = select k1, a1 from Input1;
$t2 = select k2, a2, d2, b2 as one, c2 as two from Input2;

insert into Output
select
    a.*,
    b.a2,
    b.d2
from $t1 as a left join $t2 as b on a.k1 = b.k2;
