/* postgres can not */
SELECT
    k,
    SUM(k) OVER w1 AS s1,
    SUM(k) OVER w2 AS s2
FROM
    as_table(AsList(AsStruct(1 AS k), AsStruct(2 AS k)))
WINDOW
    w1 AS (
        ORDER BY
            k
    ),
    w2 AS (
        ORDER BY
            k DESC
    )
ORDER BY
    k
;
