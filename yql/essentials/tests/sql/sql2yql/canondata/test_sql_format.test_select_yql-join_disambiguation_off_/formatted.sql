PRAGMA YqlSelect = 'force';

SELECT
    x.a
FROM (
    SELECT
        1 AS a
) AS x;
