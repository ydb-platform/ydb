/* syntax version 1 */
/* postgres can not */
PRAGMA warning("disable", "4520");
PRAGMA warning("disable", "4521");
PRAGMA AnsiRankForNullableKeys;

SELECT
    key,
    subkey,
    RANK() OVER w1 AS r1,
    DENSE_RANK() OVER w1 AS r2,
    RANK() OVER w2 AS r3,
    DENSE_RANK() OVER w2 AS r4,
FROM plato.Input
WINDOW
    w1 AS (
        PARTITION BY
            key
        ROWS BETWEEN UNBOUNDED PRECEDING AND 5 PRECEDING
    ),
    w2 AS (
        ROWS BETWEEN 3 FOLLOWING AND 2 FOLLOWING
    )
ORDER BY
    key,
    subkey;
