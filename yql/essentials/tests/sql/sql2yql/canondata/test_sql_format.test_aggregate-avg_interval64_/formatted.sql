/* skip_forceblocks */
SELECT
    EnsureType(Avg(CAST(key AS Interval64)), Interval64?)
FROM (
    SELECT
        Interval64('P1D') AS key
);

SELECT
    EnsureType(AvgIf(CAST(key AS Interval64), key > Interval64('P1D')), Interval64?)
FROM (
    SELECT
        Interval64('P1D') AS key
);
