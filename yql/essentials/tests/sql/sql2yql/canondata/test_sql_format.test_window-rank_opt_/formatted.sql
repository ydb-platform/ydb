/* syntax version 1 */
/* postgres can not */
PRAGMA warning("disable", "4520");
PRAGMA AnsiRankForNullableKeys;

SELECT
    key,
    subkey,
    RANK() OVER w1 AS r1,
    DENSE_RANK() OVER w1 AS r2,
    RANK(subkey) OVER w1 AS r3,
    DENSE_RANK(subkey) OVER w1 AS r4,
    RANK() OVER w2 AS r5,
    DENSE_RANK() OVER w2 AS r6,
    RANK(subkey || subkey) OVER w2 AS r7,
    DENSE_RANK(subkey || subkey) OVER w2 AS r8,
FROM (
    SELECT
        CAST(key AS uint32) AS key,
        subkey,
        value
    FROM plato.Input
    WHERE key == '1'
)
WINDOW
    w1 AS (
        PARTITION BY
            key
        ORDER BY
            subkey
        ROWS BETWEEN UNBOUNDED PRECEDING AND 5 PRECEDING
    ),
    w2 AS (
        ORDER BY
            key,
            subkey
        ROWS BETWEEN 3 FOLLOWING AND 2 FOLLOWING
    )
ORDER BY
    key,
    subkey;
