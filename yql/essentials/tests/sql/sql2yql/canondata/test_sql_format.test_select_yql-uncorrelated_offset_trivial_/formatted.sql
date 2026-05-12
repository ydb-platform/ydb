/* custom error: Subquery expression is not supported at offset */
PRAGMA YqlSelect = 'force';

-- FIXME(YQL-20436): bad test.
SELECT
    a
FROM (
    VALUES
        (1, 10),
        (2, 20),
        (3, 30)
) AS y (
    a,
    b
)
ORDER BY
    a
LIMIT 1 OFFSET (
    SELECT
        2
);
