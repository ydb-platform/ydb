SELECT
    EnsureType(Avg(CAST(key AS Interval64)) OVER (), Interval64)
FROM (
    SELECT
        Interval64('P1D') AS key
);
