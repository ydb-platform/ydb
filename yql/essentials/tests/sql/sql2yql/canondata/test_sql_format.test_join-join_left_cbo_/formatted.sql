USE plato;

PRAGMA warning('disable', '8001'); -- CBO_MISSING_TABLE_STATS
PRAGMA CostBasedOptimizer = 'PG';

SELECT
    i1.value,
    i2.value
FROM
    Input1 AS i1
LEFT JOIN
    Input2 AS i2
ON
    i1.key == i2.key
ORDER BY
    i1.value,
    i2.value
;
