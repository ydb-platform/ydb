/* postgres can not */
select distinct avg_key, min_val from (
    select avg(cast(key as int)) as avg_key, min(value) as min_val from plato.Input3 group by subkey
) as x order by avg_key, min_val;