/* postgres can not */
/* syntax version 1 */
USE plato;
PRAGMA DisableSimpleColumns;

--INSERT INTO Output
SELECT
    hundred_keys,
    sum(CAST(`data.subkey` AS uint32))
FROM (
    SELECT
        data.key AS dkey,
        data.*
    FROM Input
        AS data
)
    AS middle
GROUP BY
    CAST(middle.dkey AS uint32) / 100 AS hundred_keys
ORDER BY
    hundred_keys;
