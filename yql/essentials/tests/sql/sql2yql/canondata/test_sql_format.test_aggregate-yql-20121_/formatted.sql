$s = (
    SELECT
        x,
        y,
        sum(z) AS cnt
    FROM (
        SELECT
            1 AS x,
            2 AS y,
            10 AS z
    )
    GROUP BY
        x,
        y
);

$s2 = (
    SELECT
        x,
        y,
        sum(cnt)
    FROM
        $s
    GROUP BY
        x,
        y
);

SELECT
    x,
    count(*)
FROM
    $s2
GROUP BY
    x
;
