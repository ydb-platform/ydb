/* postgres can not */
/* syntax version 1 */
-- YQL-1977
USE plato;

--insert into Output
SELECT
    key_mod,
    aggr_list(value) OVER w,
    aggr_list(subkey) OVER w
FROM
    Input
WINDOW
    w AS (
        PARTITION BY
            CAST(key AS uint32) % 10 AS key_mod
        ORDER BY
            subkey
    )
ORDER BY
    key_mod,
    column1
;
