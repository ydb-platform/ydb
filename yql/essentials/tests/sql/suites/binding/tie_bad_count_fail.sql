/* syntax version 1 */
/* postgres can not */
/* custom error:Expected tuple type of size: 2, but got: 3*/
$i, $j = AsTuple(1, 5u, "test");

select $i, $j;
