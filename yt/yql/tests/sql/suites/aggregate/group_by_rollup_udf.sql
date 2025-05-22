/* syntax version 1 */
/* postgres can not */
use plato;

select key, subkey, Unicode::ToUpper(CAST(value AS Utf8)) as value, count(1) as cnt from Input GROUP BY ROLLUP(key,subkey, value) ORDER BY key,subkey,value,cnt;

