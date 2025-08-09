$run_id_limit = CAST(CurrentUtcTimestamp() AS Uint64) - 30UL * 86400UL * 1000000UL;

$fail_tests = SELECT
    Db,
    Suite,
    RunId,
    ListConcat(
        ListSort(
            AGG_LIST(
                IF(
                    Test = "_Verification",
                    "Infrastructure error",
                    IF(
                        StartsWith(Test, 'Query'),
                        SubString(CAST(Test AS String), 5U),
                        CAST(Test AS String)
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
                IF(
                    StartsWith(Test, 'Query'),
                    SubString(CAST(Test AS String), 5U),
                    CAST(Test AS String)
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
    MAX_BY(JSON_VALUE(Info, "$.ci_version"), Success) AS CiVersion,
    MAX_BY(JSON_VALUE(Info, "$.test_tools_version"), Success) AS TestToolsVersion,
    MAX_BY(JSON_VALUE(Info, "$.report_url"), Success) AS Report,
    SUM_IF(MeanDuration, Success > 0 AND Test not in {"_Verification", "Sum"} AND JSON_VALUE(Stats, '$.import_time') IS NULL) / 1000. AS YdbSumMeans,
    SUM_IF(MeanDuration * CAST(JSON_VALUE(Stats, '$.SuccessCount') AS int), Success > 0 AND Test not in {"_Verification", "Sum"} AND JSON_VALUE(Stats, '$.import_time') IS NULL) / 1000. AS QuasiGrossTime,
    SUM_IF(COALESCE(CAST(JSON_VALUE(Stats, '$.time_with_compaction') AS Float)), Test not in {"_Verification", "Sum"}) AS SumImportWithCompactionTime,
    SUM_IF(COALESCE(CAST(JSON_VALUE(Stats, '$.import_time') AS Float)), Test not in {"_Verification", "Sum"}) AS SumImportTime,
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
    s.CiVersion AS CiVersion,
    s.TestToolsVersion AS TestToolsVersion,
    s.Report AS Report,
    s.YdbSumMeans AS YdbSumMeans,
    s.SumImportTime AS SumImportTime,
    s.SumImportWithCompactionTime AS SumImportWithCompactionTime,
    s.SumCompactedBytes AS SumCompactedBytes,
    s.SumWrittenBytes AS SumWrittenBytes,
    IF(COALESCE(s.GrossTime) > 0, s.GrossTime, s.QuasiGrossTime) AS GrossTime,
    s.SuccessCount AS SuccessCount,
    s.FailCount AS FailCount,
    s.AvgImportSpeed AS AvgImportSpeed,
    s.AvgCpuCores AS AvgCpuCores,
    s.AvgCpuTime AS AvgCpuTime,
    s.Begin AS Begin,
    s.End AS End,
    d.DiffTests AS DiffTests,
    f.FailTests AS FailTests,
    CASE
        WHEN s.Db LIKE '%sas%' THEN 'sas'
        WHEN s.Db LIKE '%vla%' THEN 'vla'
        ELSE 'other'
    END AS DbDc,

    CASE
        WHEN s.Db LIKE '%load%' THEN 'column'
        WHEN s.Db LIKE '%/s3%' THEN 's3'
        WHEN s.Db LIKE '%/row%' THEN 'row'
        ELSE 'other'
    END AS DbType,

    CASE
        WHEN s.Db LIKE '%sas-daily%' THEN 'sas_small_'
        WHEN s.Db LIKE '%sas-perf%' THEN 'sas_big_'
        WHEN s.Db LIKE '%sas%' THEN 'sas_'
        WHEN s.Db LIKE '%vla-acceptance%' THEN 'vla_small_'
        WHEN s.Db LIKE '%vla-perf%' THEN 'vla_big_'
        WHEN s.Db LIKE '%vla4-8154%' THEN 'vla_8154_'
        WHEN s.Db LIKE '%vla4-8161%' THEN 'vla_8161_'
        WHEN s.Db LIKE '%vla%' THEN 'vla_'
        ELSE 'new_db_'
    END || CASE
        WHEN s.Db LIKE '%load%' THEN 'column'
        WHEN s.Db LIKE '%/s3%' THEN 's3'
        WHEN s.Db LIKE '%/row%' THEN 'row'
        ELSE 'other'
    END AS DbAlias,
    COALESCE(SubString(CAST(s.Version AS String), 0U, FIND(CAST(s.Version AS String), '.')), 'unknown') As Branch,
    COALESCE(SubString(CAST(s.CiVersion AS String), 0U, FIND(CAST(s.CiVersion AS String), '.')), 'unknown') As CiBranch,
    COALESCE(SubString(CAST(s.TestToolsVersion AS String), 0U, FIND(CAST(s.TestToolsVersion AS String), '.')), 'unknown') As TestToolsBranch
FROM $suites AS s
LEFT JOIN $diff_tests AS d ON s.RunId = d.RunId AND s.Db = d.Db AND s.Suite = d.Suite
LEFT JOIN $fail_tests AS f ON s.RunId = f.RunId AND s.Db = f.Db AND s.Suite = f.Suite
