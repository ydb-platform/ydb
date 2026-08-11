/* custom error: Uncompatible member column0 types: Int32 and String */
PRAGMA YqlSelect = 'force';

WITH x (a) AS (
    SELECT
        1
    UNION ALL
    SELECT
        '2'
)
SELECT
    *
FROM
    x
;
