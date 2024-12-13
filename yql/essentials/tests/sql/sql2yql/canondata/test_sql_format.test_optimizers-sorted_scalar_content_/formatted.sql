USE plato;

$ou =
    SELECT
        *
    FROM
        Input
;

$a =
    SELECT
        key
    FROM
        $ou
    WHERE
        key > '0'
;

INSERT INTO @a
SELECT
    *
FROM
    $a
ORDER BY
    key
;

SELECT
    *
FROM
    $ou
WHERE
    subkey > '0'
    AND key != $a
ORDER BY
    key
;
