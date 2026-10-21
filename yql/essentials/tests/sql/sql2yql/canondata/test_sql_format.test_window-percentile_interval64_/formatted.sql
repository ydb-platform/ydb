SELECT
    EnsureType(Percentile(CAST(key AS Interval64), 0.5) OVER (), Interval64)
FROM (
    SELECT
        Interval64('P1D') AS key
);
