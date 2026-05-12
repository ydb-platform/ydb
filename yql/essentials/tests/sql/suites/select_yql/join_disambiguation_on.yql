PRAGMA YqlSelect = 'force';

SELECT
    x.a
FROM (
    SELECT
        1 AS a
) AS x
JOIN (
    SELECT
        1 AS a
) AS y
ON
    x.a == y.a
;
