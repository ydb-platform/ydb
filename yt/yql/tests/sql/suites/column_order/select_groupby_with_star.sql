/* postgres can not */
/* syntax version 1 */
use plato;
pragma OrderedColumns;

select * from Input group by value, key order by key, value;
select * from Input group by value, key having key = "150";
select * from Input group by subkey, key || "x" as key order by subkey, key;
