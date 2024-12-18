/* postgres can not */
SELECT
    key,
    FIRST_VALUE(CAST(subkey AS uint32)) RESPECT NULLS OVER w1,
    FIRST_VALUE(CAST(subkey AS uint32)) IGNORE NULLS OVER w1,
    LAST_VALUE(CAST(subkey AS uint32)) OVER w1,
    LAST_VALUE(CAST(subkey AS uint32)) IGNORE NULLS OVER w1,
    subkey
FROM
    plato.Input
WINDOW
    w1 AS (
        ORDER BY
            key DESC,
            subkey
    )
;
