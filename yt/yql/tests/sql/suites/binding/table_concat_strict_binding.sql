/* syntax version 1 */
/* postgres can not */
$a = "Input";
SELECT count(*) FROM plato.concat_strict($a,$a);

USE plato;

$a = "Input";
SELECT count(*) FROM concat_strict($a,$a);
