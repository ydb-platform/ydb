USE plato;

$sub = (
    SELECT
        key
    FROM Input
    WHERE value == 'abc'
);

INSERT INTO Output
SELECT
    value,
    $sub AS s
FROM Input
ORDER BY
    value;
