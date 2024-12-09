/* postgres can not */

$a = (select null as a, 1 as b);
$b = (select null as a, 1 as b);

select a.*
from $a as a
left only join $b as b
using(a);
