SELECT
    FormatType(TypeOf(some(DISTINCT DateTime::ShiftMonths(x, 1))))
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
