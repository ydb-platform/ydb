/* postgres can not */
USE plato;

$data = (
    SELECT
        max_by(key, subkey)
    FROM Input
    WHERE value > "a"
);

SELECT
    a.key,
    $data AS max_key,
    b.value
FROM Input
    AS a
LEFT JOIN (
    SELECT
        *
    FROM Input
    WHERE key > "050"
)
    AS b
ON a.key == b.key
ORDER BY
    a.key;
