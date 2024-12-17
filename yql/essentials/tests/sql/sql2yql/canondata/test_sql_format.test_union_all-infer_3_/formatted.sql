/* postgres can not */
USE plato;

SELECT
    Just(1) AS x,
    1 AS y
UNION ALL
SELECT
    Just(1l) AS x,
    2 AS y
UNION ALL
SELECT
    3 AS y
;
