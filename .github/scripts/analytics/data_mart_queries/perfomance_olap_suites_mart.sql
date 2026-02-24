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
    SUM_IF(COALESCE(CAST(JSON_VALUE(Stats, '$.CompilationAvg') AS Float)), Test not in {"_Verification", "Sum"}) AS SumCompilationTime,
    SUM_IF(COALESCE(CAST(JSON_VALUE(Stats, '$.compacted_bytes') AS Float)), Test not in {"_Verification", "Sum"}) AS SumCompactedBytes,
    SUM_IF(COALESCE(CAST(JSON_VALUE(Stats, '$.written_bytes') AS Float)), Test not in {"_Verification", "Sum"}) AS SumWrittenBytes,
    SUM_IF(COALESCE(CAST(JSON_VALUE(Stats, '$.GrossTime') AS float)), Test = 'Sum') AS GrossTime,
    COUNTIF(COALESCE(CAST(JSON_VALUE(Stats, '$.FailsCount') AS int), 1 - Success) = 0) AS SuccessCount,
    COUNTIF(COALESCE(CAST(JSON_VALUE(Stats, '$.FailsCount') AS int), 1 - Success) > 0) AS FailCount,
    AVG_IF(COALESCE(CAST(JSON_VALUE(Stats, '$.import_speed') AS float)), Success > 0 AND Test not in {"_Verification", "Sum"}) / 1. AS AvgImportSpeed,
    AVG_IF(COALESCE(CAST(JSON_VALUE(Stats, '$.cpu_cores') AS float)), Success > 0 AND Test not in {"_Verification", "Sum"}) / 1. AS AvgCpuCores,
    AVG_IF(COALESCE(CAST(JSON_VALUE(Stats, '$.cpu_time') AS float)), Success > 0 AND Test not in {"_Verification", "Sum"}) / 1. AS AvgCpuTime,
    SUM_IF(COALESCE(CAST(JSON_VALUE(Stats, '$.satisfaction_avg_test_pool_30') AS float)), Success > 0 AND Test not in {"_Verification", "Sum"}) AS Satisfaction30,
    SUM_IF(COALESCE(CAST(JSON_VALUE(Stats, '$.satisfaction_avg_test_pool_40') AS float)), Success > 0 AND Test not in {"_Verification", "Sum"}) AS Satisfaction40,
    SUM_IF(COALESCE(CAST(JSON_VALUE(Stats, '$.satisfaction_avg_test_pool_50') AS float)), Success > 0 AND Test not in {"_Verification", "Sum"}) AS Satisfaction50,
    SUM_IF(COALESCE(CAST(JSON_VALUE(Stats, '$.satisfaction_avg_test_pool_100') AS float)), Success > 0 AND Test not in {"_Verification", "Sum"}) AS Satisfaction100,
    SUM_IF(COALESCE(CAST(JSON_VALUE(Stats, '$.tpcc_efficiency') AS float)), Success > 0 AND Test not in {"_Verification", "Sum"}) AS TpccEfficiency,
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
    s.SumCompilationTime AS SumCompilationTime,
    s.SumImportWithCompactionTime AS SumImportWithCompactionTime,
    s.SumCompactedBytes AS SumCompactedBytes,
    s.SumWrittenBytes AS SumWrittenBytes,
    IF(COALESCE(s.GrossTime) > 0, s.GrossTime, s.QuasiGrossTime) AS GrossTime,
    s.SuccessCount AS SuccessCount,
    s.FailCount AS FailCount,
    s.AvgImportSpeed AS AvgImportSpeed,
    s.AvgCpuCores AS AvgCpuCores,
    s.AvgCpuTime AS AvgCpuTime,
    s.Satisfaction30 * IF(s.Satisfaction30 > 1., 1.e-6, 1.) AS Satisfaction30,
    s.Satisfaction40 * IF(s.Satisfaction40 > 1., 1.e-6, 1.) AS Satisfaction40,
    s.Satisfaction50 * IF(s.Satisfaction50 > 1., 1.e-6, 1.) AS Satisfaction50,
    s.Satisfaction100 * IF(s.Satisfaction100 > 1., 1.e-6, 1.) AS Satisfaction100,
    s.TpccEfficiency AS TpccEfficiency,
    s.Begin AS Begin,
    s.End AS End,
    d.DiffTests AS DiffTests,
    f.FailTests AS FailTests,
    CASE
        WHEN s.Db LIKE '%sas%' THEN 'sas'
        WHEN s.Db LIKE '%vla%' THEN 'vla'
        WHEN s.Db LIKE '%klg%' THEN 'klg'
        WHEN s.Db LIKE '%etn0vb1kg3p016q1tp3t%' THEN 'cloud'
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
        WHEN s.Db LIKE '%vla4-8154%' THEN 'vla_2_node_'
        WHEN s.Db LIKE '%vla4-8157%' THEN 'vla_1_node_'
        WHEN s.Db LIKE '%vla4-8163%' THEN 'vla_3_node_'
        WHEN s.Db LIKE '%vla%' THEN 'vla_'
        WHEN s.Db LIKE '%etn0vb1kg3p016q1tp3t%b1ggceeul2pkher8vhb6/etn0vb1kg3p016q1tp3t%' THEN 'cloud_slonnn_128_'
        WHEN s.Db LIKE '%etntj9d0t8v7ud2hrqho%b1ggceeul2pkher8vhb6/etntj9d0t8v7ud2hrqho%' THEN 'cloud_slonnn_64_'
        WHEN s.Db LIKE '%static-node-1.ydb-cluster.com/Root/db%' THEN 'ansible_'
        WHEN s.Db LIKE '%ydb-vla-dev04-002%' THEN 'oltp-vla-perf1_'
        WHEN s.Db LIKE '%ydb-vla-dev04-007%' THEN 'oltp-vla-perf2_'
        WHEN s.Db LIKE '%ydb-qa-01-klg-010%' THEN 'oltp-klg-perf3_'
        WHEN s.Db LIKE '%ydb-qa-01-klg-014%' THEN 'oltp-klg-perf4_'
        WHEN s.Db LIKE '%ydb-qa-01-klg-018%' THEN 'oltp-klg-perf5_'
        WHEN s.Db LIKE '%ydb-qa-01-vla-000%' THEN 'oltp-3dc-perf6_'
        WHEN s.Db LIKE '%ydb-qa-01-klg-023%' THEN 'oltp-klg-perf7_'
        WHEN s.Db LIKE '%ydb-qa-01-klg-032%' THEN 'oltp-klg-perf9_'
        ELSE 'new_db_'
    END || CASE
        WHEN s.Db LIKE '%load%' THEN 'column'
        WHEN s.Db LIKE '%/s3%' THEN 's3'
        WHEN s.Db LIKE '%/row%' THEN 'row'
        ELSE 'other'
    END AS DbAlias,
    COALESCE(SubString(CAST(s.Version AS String), 0U, RFIND(CAST(s.Version AS String), '.')), 'unknown') As Branch,
    COALESCE(SubString(CAST(s.CiVersion AS String), 0U, RFIND(CAST(s.CiVersion AS String), '.')), 'unknown') As CiBranch,
    COALESCE(SubString(CAST(s.TestToolsVersion AS String), 0U, RFIND(CAST(s.TestToolsVersion AS String), '.')), 'unknown') As TestToolsBranch
FROM $suites AS s
LEFT JOIN $diff_tests AS d ON s.RunId = d.RunId AND s.Db = d.Db AND s.Suite = d.Suite
LEFT JOIN $fail_tests AS f ON s.RunId = f.RunId AND s.Db = f.Db AND s.Suite = f.Suite
