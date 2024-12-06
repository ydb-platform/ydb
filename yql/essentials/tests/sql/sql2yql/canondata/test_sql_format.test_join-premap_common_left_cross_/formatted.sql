PRAGMA DisableSimpleColumns;
PRAGMA DisablePullUpFlatMapOverJoin;
USE plato;

$one = (
    SELECT
        key,
        1 AS subkey,
        value
    FROM
        Input1
);

$two = (
    SELECT
        key,
        value
    FROM
        Input2
);

$three = (
    SELECT
        key,
        value,
        2 AS subkey
    FROM
        Input3
);

FROM
    $one AS a
CROSS JOIN
    $two AS b
LEFT JOIN
    $three AS c
ON
    (c.key == a.key AND c.value == b.value)
SELECT
    *
ORDER BY
    a.key,
    a.subkey,
    b.key,
    b.value
;
