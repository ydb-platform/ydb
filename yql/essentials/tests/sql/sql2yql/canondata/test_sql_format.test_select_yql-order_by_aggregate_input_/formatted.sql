PRAGMA YqlSelect = 'force';

SELECT
    k,
    -sum(x) AS x
FROM (
    VALUES
        (1, 10),
        (1, 20),
        (2, 5)
) AS t (
    k,
    x
)
GROUP BY
    k
ORDER BY
    sum(x),
    k
;

SELECT
    k,
    -sum(x) AS x
FROM (
    VALUES
        (1, 10),
        (1, 20),
        (2, 5)
) AS t (
    k,
    x
)
GROUP BY
    k
ORDER BY
    sum(t.x),
    k
;

SELECT
    k,
    -sum(x) AS x
FROM (
    VALUES
        (1, 10),
        (1, 20),
        (2, 5)
) AS t (
    k,
    x
)
GROUP BY
    k
ORDER BY
    x,
    k
;
