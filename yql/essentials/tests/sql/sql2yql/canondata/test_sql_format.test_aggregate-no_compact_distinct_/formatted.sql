USE plato;

PRAGMA AnsiOptionalAs;

$x = (
    SELECT
        key,
        AVG(DISTINCT CAST(subkey AS float)) s
    FROM
        InputB
    GROUP BY
        key
);

$y = (
    SELECT
        key,
        SUM(CAST(subkey AS float)) s
    FROM
        InputC
    GROUP BY
        key
);

SELECT
    x.key,
    x.s AS s1,
    y.s AS s2
FROM
    $x x
FULL OUTER JOIN
    $y y
ON
    x.key == y.key
;
