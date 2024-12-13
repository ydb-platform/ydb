/* syntax version 1 */
/* dq can not */
/* dqfile can not */
/* yt can not */
USE plato;

PRAGMA AnsiOptionalAs;

SELECT
    key subkey,
    value v,
FROM
    Input
ORDER BY
    subkey
;

SELECT
    key AS subkey,
    value v,
FROM
    Input
ORDER BY
    subkey
;
