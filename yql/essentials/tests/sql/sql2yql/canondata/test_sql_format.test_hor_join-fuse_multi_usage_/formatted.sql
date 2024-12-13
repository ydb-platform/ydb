USE plato;

$input =
    SELECT
        key,
        some(subkey) AS subkey,
        some(value) AS value
    FROM
        Input
    GROUP BY
        key
;

SELECT
    key
FROM
    $input
WHERE
    subkey > '0'
ORDER BY
    key
;

INSERT INTO @a
SELECT
    t.*,
    RandomNumber(TableRow()) AS rnd
FROM
    $input AS t
WHERE
    value > 'a'
;

INSERT INTO @b
SELECT
    *
FROM
    $input
;
