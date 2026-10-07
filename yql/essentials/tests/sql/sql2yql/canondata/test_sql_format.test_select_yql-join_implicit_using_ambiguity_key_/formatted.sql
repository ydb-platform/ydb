/* custom error: Column reference is ambiguous: a */
PRAGMA YqlSelect = 'force';

SELECT
    *
FROM (
    VALUES
        (1, 2)
) AS x (
    a,
    cx
)
JOIN (
    VALUES
        (1, 3)
) AS y (
    a,
    cy
)
ON
    a == a
;
