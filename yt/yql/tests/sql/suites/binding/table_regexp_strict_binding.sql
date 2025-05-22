/* syntax version 1 */
/* postgres can not */
/* kikimr can not - range not supported */

$a = "";
$b = "Input";
$c = "";
$d = "";
SELECT count(*) FROM plato.regexp_strict($a,$b,$c,$d);

use plato;

$a = "";
$b = "Input";
$c = "";
$d = "";
SELECT count(*) FROM regexp_strict($a,$b,$c,$d);
