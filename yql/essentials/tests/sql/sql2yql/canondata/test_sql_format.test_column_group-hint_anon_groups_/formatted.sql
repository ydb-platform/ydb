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

-- Forces specific group for $i
INSERT INTO @tmp WITH column_groups = '{grp=[b;c;d]}'
SELECT
    *
FROM
    $i
;
