USE plato;

$a =
    SELECT
        max(key)
    FROM
        Input
;

$b =
    SELECT
        min(subkey)
    FROM
        Input
    WHERE
        key < $a
;

$c =
    SELECT
        min(key)
    FROM
        Input
    WHERE
        key < '080'
;

SELECT
    *
FROM
    Input
WHERE
    key > $c AND subkey > $b
;
