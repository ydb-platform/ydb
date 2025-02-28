/* syntax version 1 */
SELECT
    DateTime::MakeDate32(DateTime::Split(fdate)) AS rdate32,
    DateTime::MakeDatetime64(DateTime::Split(fdatetime)) AS rdatetime64,
    DateTime::MakeTimestamp64(DateTime::Split(ftimestamp)) AS rtimestamp64,
    DateTime::MakeTzDate32(DateTime::Split(ftzpdate)) AS rtzpdate32,
    DateTime::MakeTzDatetime64(DateTime::Split(ftzpdatetime)) AS rtzpdatetime64,
    DateTime::MakeTzTimestamp64(DateTime::Split(ftzptimestamp)) AS rtzptimestamp64,
    DateTime::MakeTzDate32(DateTime::Split(ftzmdate)) AS rtzmdate32,
    DateTime::MakeTzDatetime64(DateTime::Split(ftzmdatetime)) AS rtzmdatetime64,
    DateTime::MakeTzTimestamp64(DateTime::Split(ftzmtimestamp)) AS rtzmtimestamp64,
FROM (
    SELECT
        CAST(fdate AS Date) AS fdate,
        CAST(fdatetime AS Datetime) AS fdatetime,
        CAST(ftimestamp AS Timestamp) AS ftimestamp,
        CAST(ftzpdate AS TzDate) AS ftzpdate,
        CAST(ftzpdatetime AS TzDatetime) AS ftzpdatetime,
        CAST(ftzptimestamp AS TzTimestamp) AS ftzptimestamp,
        CAST(ftzmdate AS TzDate) AS ftzmdate,
        CAST(ftzmdatetime AS TzDatetime) AS ftzmdatetime,
        CAST(ftzmtimestamp AS TzTimestamp) AS ftzmtimestamp,
    FROM Input
);
