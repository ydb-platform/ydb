/* syntax version 1 */
/* kikimr can not */
pragma yt.InferSchema;

/* postgres can not */
use plato;
$x = "Input";
SELECT count(*) FROM $x;
