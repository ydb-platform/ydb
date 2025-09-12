/* postgres can not */
/* syntax version 1 */
USE plato;

$list = select ListSort(aggregate_list(key)) from Input;

SELECT * FROM Input
ORDER BY ListIndexOf($list ?? [], key);
