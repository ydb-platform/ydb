/* postgres can not */
/* syntax version 1 */

$single = (select key from plato.Input);
$all = (select * from $single order by key limit 100);
select $all;

