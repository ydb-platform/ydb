/* syntax version 1 */
/* postgres can not */
select count(1), key, subkey, value, grouping(key, subkey, value) from plato.Input group by grouping sets ((), value, rollup(key, subkey), ())
order by key, subkey, value;
