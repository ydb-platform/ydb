/* syntax version 1 */
/* postgres can not */
USE plato;

--INSERT INTO Output
SELECT
    key,
    sk,
    aggregate_list(value) AS values
FROM (
    SELECT
        *
    FROM Input
)
GROUP BY
    key,
    CAST(subkey AS uint32) % 2 AS sk
ORDER BY
    key,
    sk;
