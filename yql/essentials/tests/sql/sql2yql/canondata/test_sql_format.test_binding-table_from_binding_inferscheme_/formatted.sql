/* syntax version 1 */
/* kikimr can not */
PRAGMA yt.InferSchema;

/* postgres can not */
USE plato;

$x = 'Input';

SELECT
    count(*)
FROM
    $x
;
