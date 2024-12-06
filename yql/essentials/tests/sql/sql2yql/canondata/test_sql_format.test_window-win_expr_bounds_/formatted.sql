/* syntax version 1 */
/* postgres can not */
USE plato;
DECLARE $begin AS Int32;

SELECT
    key,
    subkey,
    COUNT(*) OVER w AS cnt
FROM
    Input4
WINDOW
    w AS (
        ORDER BY
            key,
            subkey
        ROWS BETWEEN $begin PRECEDING AND 1 + 1 FOLLOWING
    )
ORDER BY
    key,
    subkey
;
