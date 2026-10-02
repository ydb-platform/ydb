PRAGMA YqlSelect = 'force';

SELECT
    Sum(a),
    (
        SELECT
            Sum(b)
        FROM (
            SELECT
                1 AS bk,
                1 AS b
        )
        GROUP BY
            bk
    )
FROM (
    SELECT
        1 AS a
)
WHERE
    a == (
        SELECT
            Sum(c)
        FROM (
            SELECT
                1 AS ck,
                1 AS c
        )
        GROUP BY
            ck
    )
    AND a IN (
        SELECT
            Sum(d)
        FROM (
            SELECT
                1 AS dk,
                1 AS d
        )
        GROUP BY
            dk
    ) AND EXISTS (
        SELECT
            Sum(e)
        FROM (
            SELECT
                1 AS ek,
                1 AS e
        )
        GROUP BY
            ek
    )
HAVING
    Sum(a) == 1
;
