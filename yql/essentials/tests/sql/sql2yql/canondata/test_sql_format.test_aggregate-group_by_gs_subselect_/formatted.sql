/* syntax version 1 */
/* postgres can not */
USE plato;

$sub = (
    SELECT
        Sum(CAST(subkey AS Uint32)) AS sumLen,
        key,
        value,
        Grouping(key, value) AS grouping
    FROM Input
    GROUP BY
        GROUPING SETS (
            (key),
            (value))
);

--INSERT INTO Output
SELECT
    t.sumLen,
    t.key,
    t.value,
    t.grouping
FROM $sub
    AS t
ORDER BY
    t.key,
    t.value;
