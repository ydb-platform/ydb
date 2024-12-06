/* postgres can not */
/* syntax version 1 */
USE plato;
PRAGMA DisableAnsiRankForNullableKeys;

--INSERT INTO Output
SELECT
    RANK() OVER w AS rank_noarg,
    DENSE_RANK() OVER w AS dense_rank_noarg,
    RANK(subkey) OVER w AS rank,
    DENSE_RANK(subkey) OVER w AS dense_rank,
    zz.*
FROM
    Input4 AS zz
WINDOW
    w AS (
        PARTITION BY
            key
        ORDER BY
            subkey,
            value
    )
ORDER BY
    key,
    subkey,
    value
;
