/* postgres can not */
USE plato;
PRAGMA SimpleColumns;

--INSERT INTO Output
SELECT
    hundred_keys,
    sum(CAST(subkey AS uint32))
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
