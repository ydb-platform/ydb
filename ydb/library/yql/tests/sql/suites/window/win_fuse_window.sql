/* syntax version 1 */
/* postgres can not */
use plato;
$data = (select unwrap(cast(key as uint32)) as key, unwrap(cast(subkey as uint32)) as subkey, value, value || value as unused from Input4);

insert into @data
select * from $data;

commit;

select
    key, subkey,
    FIRST_VALUE(value) over w1 as w1_first_value,
    SUM(subkey) over w2 as w2_sum_subkey,
    LAST_VALUE(value) over w3 as w3_last_value,
    SUM(key) over w4 as w4_sum_key,
    LEAD(value) over w5 as w5_next_value,
from @data
window
    w1 as (partition by subkey, key order by value),
    w2 as (order by key,subkey rows unbounded preceding),
    w3 as (partition by key, subkey order by value rows unbounded preceding), -- = w1
    w4 as (order by key,subkey rows between unbounded preceding and current row), -- = w2
    w5 as (partition by subkey, key order by value rows between unbounded preceding and current row) -- = w1
order by key, subkey;
