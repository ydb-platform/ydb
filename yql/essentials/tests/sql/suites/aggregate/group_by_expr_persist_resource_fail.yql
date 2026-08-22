/* custom error: Expected hashable and equatable type for key column */
PRAGMA FailOnNonPersistableFlattenAndAggrExprs;

SELECT
    FormatType(TypeOf(Some(DISTINCT DateTime::ShiftMonths(x, 1))))
FROM (
    SELECT
        CurrentUtcDate() AS x
);

SELECT
    FormatType(TypeOf(y))
FROM (
    SELECT
        CurrentUtcDate() AS x
)
GROUP BY
    DateTime::ShiftMonths(x, 1) AS y
;
