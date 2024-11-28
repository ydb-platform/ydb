/* syntax version 1 */

use plato;
pragma AnsiOrderByLimitInUnionAll;

select distinct key, value from Input2
union all
select key, value from Input2 order by key, value;

