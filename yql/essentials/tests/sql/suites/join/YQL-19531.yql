$sum = ($x) -> {
    $add = ($v, $res) -> {
        RETURN $v + $res;
    };
    RETURN ListFold($x, 0, $add);
};

SELECT
    *
FROM (
    VALUES
        (1)
) AS x (
    a
)
JOIN (
    VALUES
        (1)
) AS y (
    b
)
ON
    [x.a] == [y.b]
;

SELECT
    *
FROM (
    VALUES
        (2)
) AS x (
    a
)
JOIN (
    VALUES
        (1, 1)
) AS y (
    b,
    c
)
ON
    x.a == $sum([y.b, y.c])
;
