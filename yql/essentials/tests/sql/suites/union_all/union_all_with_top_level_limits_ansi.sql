/* syntax version 1 */
/* postgres can not */

use plato;
pragma AnsiOrderByLimitInUnionAll;

$foo =
select * from Input
union all
select * from Input limit 2;

$bar =
select * from Input
union all
(select * from Input limit 2);


select * from $foo order by subkey;
select * from $bar order by subkey;

select 1 as key
union all
select 2 as key assume order by key into result aaa;

discard
select 1 as key
union all
select 2 as key assume order by key;
