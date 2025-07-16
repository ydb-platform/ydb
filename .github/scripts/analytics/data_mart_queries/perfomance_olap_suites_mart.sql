$run_id_limit = CAST(CurrentUtcTimestamp() AS Uint64) - 30UL * 86400UL * 1000000UL;

$fail_tests = SELECT
    Db,
    Suite,
    RunId,
    ListConcat(
        ListSort(
            AGG_LIST(
                SubString(CAST(Test AS String), 
                    if(
                        StartsWith(Test, '_'),
                        1U,
                        if(StartsWith(Test, 'Query'), 5U, 0U)
                    )
                )
            )
        )
        , ", "
    ) AS FailTests,
FROM `perfomance/olap/tests_results`
WHERE
   RunId / 1000 > $run_id_limit
   AND COALESCE(CAST(JSON_VALUE(Stats, '$.FailsCount') AS int), 1 - Success) > 0
GROUP BY  RunId, Db, Suite;

$diff_tests = SELECT
    Db,
    Suite,
    RunId,
    ListConcat(
        ListSort(
            AGG_LIST(
                SubString(CAST(Test AS String),
                    if(
                        StartsWith(Test, '_'),
                        1U,
                        if(StartsWith(Test, 'Query'), 5U, 0U)
                     )
                )
            )
        ),
        ", "
    ) AS DiffTests,
FROM `perfomance/olap/tests_results`
WHERE
    RunId / 1000 > $run_id_limit
    AND  CAST(JSON_VALUE(Stats, '$.DiffsCount') AS int) > 0
GROUP BY  RunId, Db, Suite;

$suites = SELECT
    Db,
    Suite,
    RunId,
    MAX_BY(JSON_VALUE(Info, "$.cluster.version"), Success) AS Version,
    MAX_BY(JSON_VALUE(Info, "$.report_url"), Success) AS Report,
    SUM_IF(MeanDuration, Success > 0 AND Test not in {"_Verification", "Sum"}) / 1000. AS YdbSumMeans,
    SUM_IF(MaxDuration, Success > 0 AND Test not in {"_Verification", "Sum"}) / 1000. AS YdbSumMax,
    SUM_IF(MinDuration, Success > 0 AND Test not in {"_Verification", "Sum"}) / 1000. AS YdbSumMin,
    SUM_IF(COALESCE(CAST(JSON_VALUE(Stats, '$.time_with_compaction') AS Float)), Test not in {"_Verification", "Sum"}) AS SumImportWithCompactionTime,
    SUM_IF(COALESCE(CAST(JSON_VALUE(Stats, '$.compacted_bytes') AS Float)), Test not in {"_Verification", "Sum"}) AS SumCompactedBytes,
    SUM_IF(COALESCE(CAST(JSON_VALUE(Stats, '$.written_bytes') AS Float)), Test not in {"_Verification", "Sum"}) AS SumWrittenBytes,
    SUM_IF(COALESCE(CAST(JSON_VALUE(Stats, '$.GrossTime') AS float)), Test = 'Sum') AS GrossTime,
    COUNTIF(COALESCE(CAST(JSON_VALUE(Stats, '$.FailsCount') AS int), 1 - Success) = 0) AS SuccessCount,
    COUNTIF(COALESCE(CAST(JSON_VALUE(Stats, '$.FailsCount') AS int), 1 - Success) > 0) AS FailCount,
    AVG_IF(COALESCE(CAST(JSON_VALUE(Stats, '$.import_speed') AS float)), Success > 0 AND Test not in {"_Verification", "Sum"}) / 1. AS AvgImportSpeed,
    AVG_IF(COALESCE(CAST(JSON_VALUE(Stats, '$.cpu_cores') AS float)), Success > 0 AND Test not in {"_Verification", "Sum"}) / 1. AS AvgCpuCores,
    AVG_IF(COALESCE(CAST(JSON_VALUE(Stats, '$.cpu_time') AS float)), Success > 0 AND Test not in {"_Verification", "Sum"}) / 1. AS AvgCpuTime,
    Min(MIN_OF(Timestamp, CAST(RunId/1000 AS Timestamp))) AS Begin,
    Max(Timestamp) AS End,
FROM `perfomance/olap/tests_results`
WHERE RunId / 1000 > $run_id_limit
GROUP BY  RunId, Db, Suite;

SELECT
    s.Db AS Db,
    s.Suite AS Suite,
    CAST(s.RunId/1000 AS Timestamp) AS RunTs,
    s.Version AS Version,
    s.Report AS Report,
    s.YdbSumMeans AS YdbSumMeans,
    s.YdbSumMax AS YdbSumMax,
    s.YdbSumMin AS YdbSumMin,
    s.SumImportWithCompactionTime AS SumImportWithCompactionTime,
    s.SumCompactedBytes AS SumCompactedBytes,
    s.SumWrittenBytes AS SumWrittenBytes,
    s.GrossTime AS GrossTime,
    s.SuccessCount AS SuccessCount,
    s.FailCount AS FailCount,
    s.AvgImportSpeed AS AvgImportSpeed,
    s.AvgCpuCores AS AvgCpuCores,
    s.AvgCpuTime AS AvgCpuTime,
    s.Begin AS Begin,
    s.End AS End,
    d.DiffTests AS DiffTests,
    f.FailTests AS FailTests
FROM $suites AS s
LEFT JOIN $diff_tests AS d ON s.RunId = d.RunId AND s.Db = d.Db AND s.Suite = d.Suite
LEFT JOIN $fail_tests AS f ON s.RunId = f.RunId AND s.Db = f.Db AND s.Suite = f.Suite
