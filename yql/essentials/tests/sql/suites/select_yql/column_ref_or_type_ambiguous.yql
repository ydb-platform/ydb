/* custom error: Column reference is ambiguous: Int64 */
PRAGMA YqlSelect = 'force';

SELECT
    Int64
FROM (
    VALUES
        (1)
) AS lhs (
    Int64
)
CROSS JOIN (
    VALUES
        (2)
) AS rhs (
    Int64
);
