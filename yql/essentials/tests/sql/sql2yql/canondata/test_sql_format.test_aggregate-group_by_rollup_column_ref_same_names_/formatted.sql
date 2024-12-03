/* syntax version 1 */
/* postgres can not */
USE plato;

$input = (
    SELECT
        CAST(key AS uint32) ?? 0 AS key,
        CAST(subkey AS int32) ?? 0 AS subkey,
        value
    FROM Input
);

--insert into Output
SELECT
    key,
    subkey,
    count(*) AS total_count
FROM $input
WHERE subkey IN (23, 37, 75, 150)
GROUP BY
    ROLLUP (key, subkey)
ORDER BY
    key,
    subkey,
    total_count;
