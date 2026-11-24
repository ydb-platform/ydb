PRAGMA YqlSelect = 'force';

SELECT
    key,
    subkey + 1
FROM
    AS_TABLE([])
;

SELECT
    1 AS x
EXCEPT ALL
SELECT
    *
FROM
    AS_TABLE([])
UNION ALL
SELECT
    2 AS x
INTERSECT ALL
SELECT
    *
FROM
    AS_TABLE([])
;
