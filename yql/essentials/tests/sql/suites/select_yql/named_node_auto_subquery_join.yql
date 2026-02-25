PRAGMA YqlSelect = 'auto';

$data = (
    SELECT
        a,
        b
    FROM (
        VALUES
            (1, 11),
            (2, 22),
            (3, 33),
    ) AS x (
        a,
        b
    )
);

SELECT DISTINCT
    x.a AS a,
    b,
    c
FROM
    $data AS x
JOIN (
    VALUES
        (1, 111),
        (2, 222),
        (3, 333)
) AS y (
    a,
    c
)
ON
    x.a == y.a
;
