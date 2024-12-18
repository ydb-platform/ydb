/* postgres can not */
USE plato;

$i = (
    SELECT
        CAST(key AS Double) AS key,
        value
    FROM
        Input
    WHERE
        key < '100'
    ORDER BY
        key DESC
    LIMIT 1000
);

SELECT DISTINCT
    key
FROM
    $i
WHERE
    value != ''
ORDER BY
    key
;
