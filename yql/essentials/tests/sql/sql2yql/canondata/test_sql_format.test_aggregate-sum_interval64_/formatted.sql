SELECT
    EnsureType(Sum(CAST(key AS Interval64)), Interval64?)
FROM (
    SELECT
        Interval64('P1D') AS key
);
