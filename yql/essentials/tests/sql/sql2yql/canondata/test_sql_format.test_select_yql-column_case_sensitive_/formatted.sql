PRAGMA YqlSelect = 'force';

SELECT
    a,
    A
FROM (
    SELECT
        1 AS a,
        2 AS A
)
WHERE
    a < A
ORDER BY
    A,
    a
;
