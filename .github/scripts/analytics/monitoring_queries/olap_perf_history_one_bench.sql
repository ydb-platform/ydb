-- https://datalens.yandex/ug26hdjlxoeoi?tab=deM
SELECT `res_0`,
         sum(CASE
    WHEN (`t1`.`AvgImportSpeed` > 0) THEN
    `t1`.`AvgImportSpeed`
    WHEN (`t1`.`GrossTime` > 0) THEN
    `t1`.`GrossTime`
    ELSE `t1`.`YdbSumMeans` END) AS `res_1`, `res_2`, sum(CAST(`t1`.`YdbSumMeans` AS BIGINT)) AS `res_3`
FROM 
    (SELECT *
    FROM `perfomance/olap/fast_results_siutes`
    WHERE RunTs > (CurrentUtcTimestamp() - Interval('P30D')) ) AS `t1`
WHERE `t1`.`Suite` IN ('Clickbench')
        AND `t1`.`TestToolsBranch` IN ('main', 'unknown')
        AND `t1`.`CiBranch` IN ('trunk', 'unknown')
        AND `t1`.`Branch` IN ('main')
        AND `t1`.`DbType` IN ('column')
        AND `t1`.`RunTs`
    BETWEEN (CurrentUtcTimestamp() - Interval('P7D'))
        AND CurrentUtcTimestamp()
GROUP BY  CAST(CAST(DateTime::MakeDatetime(`t1`.`RunTs` + DateTime::IntervalFromHours(3)) AS DATE) AS UTF8) || ' ' || CAST(DateTime::GetHour(DateTime::MakeDatetime(`t1`.`RunTs` + DateTime::IntervalFromHours(3))) AS UTF8) || ':' || CAST(DateTime::GetMinute(DateTime::MakeDatetime(`t1`.`RunTs` + DateTime::IntervalFromHours(3))) AS UTF8) || ' <a target=_blank href="' || `t1`.`Report` || '">' ||
    CASE
    WHEN String::StartsWith(`t1`.`Version`, 'main.') THEN
    Unicode::Substring(CAST(`t1`.`Version` AS UTF8), 6 - 1)
    WHEN String::StartsWith(`t1`.`Version`, 'stable-') THEN
    'stable.' || ListHead(ListSkip(Unicode::SplitToList(CAST(`t1`.`Version` AS UTF8), '.'), 2 - 1))
    ELSE `t1`.`Version`
    END || '</a>' AS `res_0`, `t1`.`DbAlias` || ' - ' || `t1`.`Branch` || ' - ' ||
    CASE
    WHEN (`t1`.`FailCount` > 0) THEN
    'with errors'
    WHEN (`t1`.`FailCount` + `t1`.`SuccessCount` < CAST(CASE
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
    END AS INTEGER) - CAST(1 AS INTEGER)) THEN
    'in_progress'
    ELSE 'finished'
    END AS `res_2` LIMIT 1000001