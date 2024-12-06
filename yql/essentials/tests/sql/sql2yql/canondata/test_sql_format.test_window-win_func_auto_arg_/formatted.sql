/* postgres can not */
/* syntax version 1 */
USE plato;
PRAGMA DisableAnsiRankForNullableKeys;

SELECT
    RANK() OVER w AS rank_noarg,
    DENSE_RANK() OVER w AS dense_rank_noarg,
    RANK(CAST(subkey AS uint32) / 10 % 2) OVER w AS rank,
    DENSE_RANK(CAST(subkey AS uint32) / 10 % 2) OVER w AS dense_rank,
    zz.*
FROM
    Input4 AS zz
WINDOW
    w AS (
        PARTITION BY
            key
        ORDER BY
            subkey
    )
ORDER BY
    key,
    subkey,
    value
;
