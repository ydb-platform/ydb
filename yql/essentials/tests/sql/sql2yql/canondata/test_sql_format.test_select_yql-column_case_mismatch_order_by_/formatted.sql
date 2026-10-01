/* custom error: No such column: a */
PRAGMA YqlSelect = 'force';

SELECT
    A
FROM (
    SELECT
        1 AS A
)
ORDER BY
    a
;
