/* custom error: Error: Wrong number of columns, expected: 3, got: 2 */
PRAGMA YqlSelect = 'force';

WITH x (a, c) AS (
    SELECT
        1,
        '2',
        TRUE
)
SELECT
    *
FROM
    x
;
