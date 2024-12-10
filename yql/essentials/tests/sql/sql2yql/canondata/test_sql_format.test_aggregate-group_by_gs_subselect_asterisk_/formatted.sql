/* syntax version 1 */
/* postgres can not */
USE plato;

$sub = (
    SELECT
        *
    FROM
        Input
    LIMIT 5
);

--INSERT INTO Output
SELECT
    Sum(CAST(subkey AS Uint32)) AS sumLen,
    key,
    value,
    Grouping(key, value) AS grouping
FROM
    $sub
GROUP BY
    GROUPING SETS (
        (key),
        (value)
    )
ORDER BY
    key,
    value
;
