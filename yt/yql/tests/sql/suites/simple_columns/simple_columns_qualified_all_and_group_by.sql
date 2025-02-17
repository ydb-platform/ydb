/* postgres can not */
USE plato;
PRAGMA SimpleColumns;

--INSERT INTO Output
SELECT
    hundred_keys, sum(cast(subkey as uint32))
FROM (
    SELECT data.key as dkey, data.*
    FROM Input as data
) as middle
GROUP BY
    cast(middle.dkey as uint32)/100 as hundred_keys
ORDER BY hundred_keys
