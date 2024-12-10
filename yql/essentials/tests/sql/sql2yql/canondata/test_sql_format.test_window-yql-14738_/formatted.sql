/* postgres can not */
/* syntax version 1 */
USE plato;

SELECT
    t.*,
    COUNT(*) OVER (
        PARTITION BY
            key || "1"
    ) AS c1,
    COUNT(*) OVER (
        PARTITION BY
            key || "2"
    ) AS c2,
    COUNT(*) OVER w AS c3,
FROM
    Input AS t
WINDOW
    w AS (
        PARTITION BY
            key || "3"
    )
ORDER BY
    subkey
;
