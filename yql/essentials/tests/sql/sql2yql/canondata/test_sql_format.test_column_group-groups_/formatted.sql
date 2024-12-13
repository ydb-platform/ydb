USE plato;

$i = (
    SELECT
        *
    FROM
        Input
    WHERE
        a > 'a'
);

SELECT
    a,
    b,
    c,
    d
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
