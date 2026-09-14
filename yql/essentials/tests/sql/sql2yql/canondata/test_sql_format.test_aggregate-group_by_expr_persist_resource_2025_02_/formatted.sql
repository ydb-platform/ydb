/* custom error: Expected hashable and equatable type for distinct column */
SELECT
    FormatType(TypeOf(some(DISTINCT DateTime::ShiftMonths(x, 1))))
FROM (
    SELECT
        CurrentUtcDate() AS x
);

/* custom error: Expected hashable and equatable type for key column */
SELECT
    FormatType(TypeOf(y))
FROM (
    SELECT
        CurrentUtcDate() AS x
)
GROUP BY
    DateTime::ShiftMonths(x, 1) AS y
;
