/* syntax version 1 */
/* postgres can not */
USE plato;

select
    key,
    value || "_y" ?? "" as value
from range("", "Input1", "Input2")
where key > "010"
order by key, value;
