/* postgres can not */
/* syntax version 1 */
USE plato;

PRAGMA TablePathPrefix = "//";

$input = "In" || "put";

SELECT
    *
FROM
    `Input`
ORDER BY
    subkey
;

SELECT
    *
FROM
    $input
ORDER BY
    subkey
;
