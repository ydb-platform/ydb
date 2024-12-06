USE plato;

PRAGMA yt.MinColumnGroupSize = "3";
PRAGMA yt.ColumnGroupMode = "perusage";

$i =
    SELECT
        *
    FROM
        Input
    WHERE
        a > "a"
;

SELECT
    a,
    b
FROM
    $i
;

SELECT
    c,
    d,
    e,
    f
FROM
    $i
;
