/* postgres can not */
/* syntax version 1 */
USE plato;

PRAGMA DisableSimpleColumns;
PRAGMA DisableAnsiRankForNullableKeys;

--INSERT INTO Output
SELECT
    RANK() OVER w AS rank_noarg,
    DENSE_RANK() OVER w AS dense_rank_noarg,
    RANK(AsTuple(key, value)) OVER w AS rank,
    DENSE_RANK(AsTuple(key, value)) OVER w AS dense_rank,
    zz.*
FROM
    Input4 AS zz
WINDOW
    w AS (
        ORDER BY
            key,
            subkey,
            value
    )
;
