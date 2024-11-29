/* postgres can not */
/* multirun can not */
USE plato;

INSERT INTO Output
SELECT
    key AS key,
    aggr_list(subkey) AS lst
FROM Input
GROUP BY
    key
ORDER BY
    key,
    lst;
COMMIT;

SELECT
    *
FROM Output
WHERE key > "150";
