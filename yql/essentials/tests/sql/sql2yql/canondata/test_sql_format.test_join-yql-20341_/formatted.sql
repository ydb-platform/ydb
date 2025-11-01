SELECT
    *
FROM (
    SELECT
        1 AS x,
        0 AS p
) AS a
CROSS JOIN (
    SELECT
        1 AS y
) AS b
INNER JOIN (
    SELECT
        1 AS z,
        0 AS q
) AS c
ON
    a.x == c.z
WHERE
    a.p == c.q
;
