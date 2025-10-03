/* syntax version 1 */
/* postgres can not */

use plato;
pragma DisableAnsiOrderByLimitInUnionAll;

$foo =
select * from Input
union all
select * from Input limit 2;

select * from $foo order by subkey;
