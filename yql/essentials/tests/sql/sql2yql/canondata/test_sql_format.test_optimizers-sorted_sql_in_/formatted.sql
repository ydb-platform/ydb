USE plato;

$ou =
    SELECT
        *
    FROM
        Input
;

$a =
    SELECT
        *
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
    AND key NOT IN COMPACT (
        SELECT
            key
        FROM
            $a
    )
ORDER BY
    key
;
