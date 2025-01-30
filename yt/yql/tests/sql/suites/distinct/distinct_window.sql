/* syntax version 1 */

use plato;

select distinct AGGREGATE_LIST(value) over w as values, key from Input2
window w as (partition by key order by value rows between unbounded preceding and unbounded following)
order by key;

