/* postgres can not */
/* syntax version 1 */
use plato;

$in = select key from InputSorted where key < "100";

select * from InputSorted where key in $in;
