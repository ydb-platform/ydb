/* syntax version 1 */
/* postgres can not */
PRAGMA warning('disable', '4520');
PRAGMA DisableAnsiRankForNullableKeys;

SELECT
    key,
    RANK() OVER w1 AS r1,
    DENSE_RANK() OVER w1 AS r2,
FROM
    AS_TABLE([<|key: 1|>, <|key: NULL|>, <|key: NULL|>, <|key: 1|>, <|key: 2|>])
WINDOW
    w1 AS (
        ORDER BY
            key
        ROWS BETWEEN UNBOUNDED PRECEDING AND 5 PRECEDING
    )
ORDER BY
    key
;
