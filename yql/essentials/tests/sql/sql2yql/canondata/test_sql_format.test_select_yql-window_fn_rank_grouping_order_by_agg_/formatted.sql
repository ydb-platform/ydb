PRAGMA YqlSelect = 'force';

SELECT
    t1.b AS b,
    t1.c AS c,
    Grouping(t1.c) AS gc,
    Rank() OVER (
        PARTITION BY
            Grouping(t1.c)
        ORDER BY
            Sum(t1.a) DESC
    ) AS rnk
FROM
    AS_TABLE([
        <|a: 1, b: 1, c: 1|>,
        <|a: 2, b: 1, c: 2|>,
        <|a: 3, b: 2, c: 1|>
    ]) AS t1
GROUP BY
    ROLLUP (
        t1.b,
        t1.c
    )
ORDER BY
    gc,
    rnk
;
