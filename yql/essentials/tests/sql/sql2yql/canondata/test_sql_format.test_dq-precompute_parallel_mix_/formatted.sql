USE plato;

$a = (
    SELECT
        key
    FROM
        Input
    ORDER BY
        key
    LIMIT 1
);

$b = (
    SELECT
        key
    FROM
        Input
    ORDER BY
        key
    LIMIT 1 OFFSET 1
);

INSERT INTO Output1
SELECT
    *
FROM
    Input
WHERE
    key <= $a
;

INSERT INTO Output2
SELECT
    *
FROM
    Input
WHERE
    key >= $a AND key != $b
;
