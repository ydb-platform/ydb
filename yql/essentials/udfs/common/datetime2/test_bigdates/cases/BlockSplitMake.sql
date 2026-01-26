/* syntax version 1 */
pragma UseBlocks;
INSERT INTO @t SELECT
    UNWRAP(CAST(fdate32 AS Date32)) AS fdate32,
    UNWRAP(CAST(fdatetime64 AS Datetime64)) AS fdatetime64,
    UNWRAP(CAST(ftimestamp64 AS Timestamp64)) AS ftimestamp64,
    UNWRAP(CAST(ftzpdate32 AS TzDate32)) AS ftzpdate32,
    UNWRAP(CAST(ftzpdatetime64 AS TzDatetime64)) AS ftzpdatetime64,
    UNWRAP(CAST(ftzptimestamp64 AS TzTimestamp64)) AS ftzptimestamp64,
    UNWRAP(CAST(ftzmdate32 AS TzDate32)) AS ftzmdate32,
    UNWRAP(CAST(ftzmdatetime64 AS TzDatetime64)) AS ftzmdatetime64,
    UNWRAP(CAST(ftzmtimestamp64 AS TzTimestamp64)) AS ftzmtimestamp64,
FROM Input;

COMMIT;

SELECT
    DateTime::MakeDate32(fdate32) AS rdate32,
    DateTime::MakeDatetime64(fdatetime64) AS rdatetime64,
    DateTime::MakeTimestamp64(ftimestamp64) AS rtimestamp64,
    DateTime::MakeTzDate32(ftzpdate32) AS rtzpdate32,
    DateTime::MakeTzDatetime64(ftzpdatetime64) AS rtzpdatetime64,
    DateTime::MakeTzTimestamp64(ftzptimestamp64) AS rtzptimestamp64,
    DateTime::MakeTzDate32(ftzmdate32) AS rtzmdate32,
    DateTime::MakeTzDatetime64(ftzmdatetime64) AS rtzmdatetime64,
    DateTime::MakeTzTimestamp64(ftzmtimestamp64) AS rtzmtimestamp64,
FROM @t;
