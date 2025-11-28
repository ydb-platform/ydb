/* postgres can not */
/* kikimr can not */
/* syntax version 1 */
USE plato;

$with_row1 = (SELECT t.*, ROW_NUMBER() OVER () as row_num FROM Input1 as t);
$with_row2 = (SELECT t.*, ROW_NUMBER() OVER () as row_num FROM Input2 as t);

SELECT a.key as key, b.subkey as subkey, b.value as value FROM $with_row1 as a LEFT JOIN $with_row2 as b USING(row_num);
