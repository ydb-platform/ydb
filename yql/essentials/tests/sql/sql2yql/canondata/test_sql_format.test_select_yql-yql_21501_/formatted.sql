/* custom error: Subqueries are not supported in JOIN ON predicate yet */
PRAGMA YqlSelect = 'force';

SELECT
    1
FROM (
    SELECT
        2 AS a,
        3 AS b
)
JOIN (
    SELECT
        4 AS a,
        5 AS b
)
ON
    (
        SELECT
            6
    )
;
