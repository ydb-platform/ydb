/* postgres can not */
/* kikimr can not - range not supported */
/* syntax version 1 */
use plato;

select t.*, TableName() as path from range("","Input1", "Input2") as t
order by path, key, value;
