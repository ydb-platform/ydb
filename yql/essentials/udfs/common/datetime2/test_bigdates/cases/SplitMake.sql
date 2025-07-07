/* syntax version 1 */
SELECT
    DateTime::MakeDate32(DateTime::Split(fdate32)) AS rdate32,
    DateTime::MakeDatetime64(DateTime::Split(fdatetime64)) AS rdatetime64,
    DateTime::MakeTimestamp64(DateTime::Split(ftimestamp64)) AS rtimestamp64,
    DateTime::MakeTzDate32(DateTime::Split(ftzpdate32)) AS rtzpdate32,
    DateTime::MakeTzDatetime64(DateTime::Split(ftzpdatetime64)) AS rtzpdatetime64,
    DateTime::MakeTzTimestamp64(DateTime::Split(ftzptimestamp64)) AS rtzptimestamp64,
    DateTime::MakeTzDate32(DateTime::Split(ftzmdate32)) AS rtzmdate32,
    DateTime::MakeTzDatetime64(DateTime::Split(ftzmdatetime64)) AS rtzmdatetime64,
    DateTime::MakeTzTimestamp64(DateTime::Split(ftzmtimestamp64)) AS rtzmtimestamp64,
FROM (
    SELECT
        CAST(fdate32 AS Date32) AS fdate32,
        CAST(fdatetime64 AS Datetime64) AS fdatetime64,
        CAST(ftimestamp64 AS Timestamp64) AS ftimestamp64,
        CAST(ftzpdate32 AS TzDate32) AS ftzpdate32,
        CAST(ftzpdatetime64 AS TzDatetime64) AS ftzpdatetime64,
        CAST(ftzptimestamp64 AS TzTimestamp64) AS ftzptimestamp64,
        CAST(ftzmdate32 AS TzDate32) AS ftzmdate32,
        CAST(ftzmdatetime64 AS TzDatetime64) AS ftzmdatetime64,
        CAST(ftzmtimestamp64 AS TzTimestamp64) AS ftzmtimestamp64,
    FROM Input
);
