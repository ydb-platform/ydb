$a = [
    AsStruct(
        'a' AS x,
        1 AS y
    ),
    AsStruct(
        'a' AS x,
        1 AS y
    ),
    AsStruct(
        'a' AS x,
        2 AS y
    ),
    AsStruct(
        'a' AS x,
        3 AS y
    ),
    AsStruct(
        'b' AS x,
        1 AS y
    ),
    AsStruct(
        'b' AS x,
        2 AS y
    ),
    AsStruct(
        'b' AS x,
        3 AS y
    ),
    AsStruct(
        'c' AS x,
        1 AS y
    ),
];

$a = (
    SELECT
        x AS bar,
        y AS foo
    FROM
        AS_TABLE($a)
);

$b = (
    SELECT
        a.bar AS bar,
        count(*) AS cnt
    FROM
        $a AS a
    INNER JOIN (
        SELECT
            bar,
            min(foo) AS foo
        FROM
            $a
        GROUP BY
            bar
    ) AS b
    USING (foo, bar)
    GROUP BY
        a.bar
);

SELECT
    *
FROM
    $a AS a
LEFT JOIN
    $b AS b
USING (bar);
