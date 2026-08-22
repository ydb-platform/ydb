$x = (
    SELECT
        *
    FROM (
        VALUES
            (1, 1)
    ) AS x (
        a,
        b
    )
);

SELECT
    *
FROM
    $x AS x
WHERE
    x.a IN (
        SELECT
            a
        FROM
            $x AS x
        WHERE
            (x.a == 1) AND x.b == 1
    )
;
