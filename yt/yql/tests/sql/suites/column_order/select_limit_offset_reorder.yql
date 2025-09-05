/* postgres can not */
/* syntax version 1 */
use plato;
pragma OrderedColumns;

select subkey, key, value from (select * from Input) as x order by key, subkey limit 1 offset 1;
