/* syntax version 1 */
/* postgres can not */
select count(1), k, subkey from plato.Input group by rollup(cast(key as uint32) as k, subkey) order by k, subkey;
