-- https://datalens.yandex/ug26hdjlxoeoi?tab=o5x
SELECT `res_0`,
         `res_1`,
         `res_2`,
         `res_3`,
         `res_4`,
         `res_5`,
         `res_6`,
         `res_7`,
         `res_8`,
         `res_9`,
         max(CAST(`t1`.`SuccessCount` AS DOUBLE) /
    CASE
    WHEN String::StartsWith(`t1`.`Suite`, 'Clickbench') THEN
    44
    WHEN String::StartsWith(`t1`.`Suite`, 'Tpch') THEN
    23
    WHEN String::StartsWith(`t1`.`Suite`, 'Tpcds') THEN
    100
    WHEN String::StartsWith(`t1`.`Suite`, 'ExternalA1') THEN
    6
    WHEN String::StartsWith(`t1`.`Suite`, 'ExternalX1') THEN
    10
    WHEN String::StartsWith(`t1`.`Suite`, 'ExternalM1') THEN
    2
    WHEN String::StartsWith(`t1`.`Suite`, 'ExternalB1') THEN
    7
    WHEN String::StartsWith(`t1`.`Suite`, 'ImportFileCsv') THEN
    2
    WHEN String::StartsWith(`t1`.`Suite`, 'WorkloadManagerClickbenchComputeScheduler') THEN
    48
    WHEN String::StartsWith(`t1`.`Suite`, 'WorkloadManagerClickbenchConcurrentQueryLimit') THEN
    48
    WHEN String::StartsWith(`t1`.`Suite`, 'WorkloadManagerTpchComputeSchedulerS100') THEN
    27
    ELSE 1 END) AS `res_10`, `res_11`
FROM 
    (SELECT *
    FROM `perfomance/olap/fast_results_siutes`
    WHERE RunTs > (CurrentUtcTimestamp() - Interval('P30D')) ) AS `t1`
WHERE `t1`.`TestToolsBranch` IN ('main', 'unknown')
        AND `t1`.`CiBranch` IN ('trunk', 'unknown')
        AND `t1`.`Branch` IN ('main')
        AND `t1`.`DbType` IN ('column')
        AND `t1`.`RunTs`
    BETWEEN (CurrentUtcTimestamp() - Interval('P13D'))
        AND CurrentUtcTimestamp()
GROUP BY  DateTime::MakeDatetime(`t1`.`RunTs` + DateTime::IntervalFromHours(3)) AS `res_0`, `t1`.`DbAlias` AS `res_1`, '(a ' || '"' || Unicode::ReplaceAll(CAST(`t1`.`Report` AS UTF8), coalesce(CAST('"' AS UTF8), ''), coalesce(CAST('""' AS UTF8), '')) || '"' || ' ' || '"' || Unicode::ReplaceAll(CAST(`t1`.`Version` AS UTF8), coalesce(CAST('"' AS UTF8), ''), coalesce(CAST('""' AS UTF8), '')) || '"' || ')' AS `res_2`, `t1`.`TestToolsVersion` AS `res_3`, `t1`.`CiVersion` AS `res_4`, `t1`.`Suite` AS `res_5`, IF(`t1`.`AvgImportSpeed` > 0, `t1`.`AvgImportSpeed`, `t1`.`YdbSumMeans`) AS `res_6`, CAST(`t1`.`SuccessCount` AS UTF8) || '/' || CAST(`t1`.`SuccessCount` + `t1`.`FailCount` AS UTF8) || '/' || CAST(CASE
    WHEN String::StartsWith(`t1`.`Suite`, 'Clickbench') THEN
    44
    WHEN String::StartsWith(`t1`.`Suite`, 'Tpch') THEN
    23
    WHEN String::StartsWith(`t1`.`Suite`, 'Tpcds') THEN
    100
    WHEN String::StartsWith(`t1`.`Suite`, 'ExternalA1') THEN
    6
    WHEN String::StartsWith(`t1`.`Suite`, 'ExternalX1') THEN
    10
    WHEN String::StartsWith(`t1`.`Suite`, 'ExternalM1') THEN
    2
    WHEN String::StartsWith(`t1`.`Suite`, 'ExternalB1') THEN
    7
    WHEN String::StartsWith(`t1`.`Suite`, 'ImportFileCsv') THEN
    2
    WHEN String::StartsWith(`t1`.`Suite`, 'WorkloadManagerClickbenchComputeScheduler') THEN
    48
    WHEN String::StartsWith(`t1`.`Suite`, 'WorkloadManagerClickbenchConcurrentQueryLimit') THEN
    48
    WHEN String::StartsWith(`t1`.`Suite`, 'WorkloadManagerTpchComputeSchedulerS100') THEN
    27
    ELSE 1
    END AS UTF8) AS `res_7`, `t1`.`FailTests` AS `res_8`, `t1`.`DiffTests` AS `res_9`, `t1`.`RunTs` AS `res_11`
ORDER BY  `res_11` DESC,
         `res_0` ASC,
         `res_1` ASC,
         `res_2` ASC,
         `res_3` ASC,
         `res_4` ASC,
         `res_5` ASC,
         `res_6` ASC,
         `res_7` ASC,
         `res_8` ASC,
         `res_9` ASC LIMIT 100 OFFSET 0