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
         max(`t1`.`OverallStatusExtended`) AS `res_10`,
         max(CASE
    WHEN (`t1`.`OverallStatus` = 'success') THEN
    1
    WHEN (`t1`.`OverallStatus` = 'failure') THEN
    5
    WHEN (`t1`.`OverallStatus` = 'workload_failure') THEN
    5
    WHEN (`t1`.`OverallStatus` = 'node_failure') THEN
    7
    WHEN (`t1`.`OverallStatus` = 'infrastructure_error') THEN
    5
    WHEN (`t1`.`OverallStatus` = 'broken') THEN
    5
    ELSE 10 END) AS `res_11`
FROM 
    (SELECT *
    FROM `nemesis/aggregated_mart` ) AS `t1`
WHERE `t1`.`CiClusterName` IN ('cross')
        AND `t1`.`RunTs` >= CurrentUtcDatetime() - 7 * Interval("P1D")
        AND `t1`.`Branch` IN ('main')
GROUP BY  coalesce('(a ' || '"' || Unicode::ReplaceAll(CAST(`t1`.`CiLaunchUrl` AS UTF8), coalesce(CAST('"' AS UTF8), ''), coalesce(CAST('""' AS UTF8), '')) || '"' || ' ' || '"' || Unicode::ReplaceAll(CAST(Unicode::ReplaceAll(CAST(ListHead(ListSkip(Unicode::SplitToList(CAST(CAST(`t1`.`RunTs` AS UTF8) AS UTF8), '.'), 1 - 1)) AS UTF8), coalesce(CAST('T' AS UTF8), ''), coalesce(CAST(' ' AS UTF8), '')) AS UTF8), coalesce(CAST('"' AS UTF8), ''), coalesce(CAST('""' AS UTF8), '')) || '"' || ')', '(c ' || '"' || Unicode::ReplaceAll(CAST(Unicode::ReplaceAll(CAST(ListHead(ListSkip(Unicode::SplitToList(CAST(CAST(`t1`.`RunTs` AS UTF8) AS UTF8), '.'), 1 - 1)) AS UTF8), coalesce(CAST('T' AS UTF8), ''), coalesce(CAST(' ' AS UTF8), '')) AS UTF8), coalesce(CAST('"' AS UTF8), ''), coalesce(CAST('""' AS UTF8), '')) || '"' || ')') AS `res_0`, '(a ' || '"' || Unicode::ReplaceAll(CAST(`t1`.`ClusterMonitoring` AS UTF8), coalesce(CAST('"' AS UTF8), ''), coalesce(CAST('""' AS UTF8), '')) || '"' || ' ' || '"' || Unicode::ReplaceAll(CAST(`t1`.`CiClusterName` AS UTF8), coalesce(CAST('"' AS UTF8), ''), coalesce(CAST('""' AS UTF8), '')) || '"' || ')' AS `res_1`, '(a ' || '"' || Unicode::ReplaceAll(CAST(`t1`.`ClusterVersionLink` AS UTF8), coalesce(CAST('"' AS UTF8), ''), coalesce(CAST('""' AS UTF8), '')) || '"' || ' ' || '"' || Unicode::ReplaceAll(CAST(`t1`.`ClusterVersion` AS UTF8), coalesce(CAST('"' AS UTF8), ''), coalesce(CAST('""' AS UTF8), '')) || '"' || ')' AS `res_2`,
    CASE
    WHEN (`t1`.`CiSanitizer` = 'address') THEN
    coalesce(`t1`.`CiBuildType`, 'release') || '-asan'
    WHEN (`t1`.`CiSanitizer` = 'thread') THEN
    coalesce(`t1`.`CiBuildType`, 'release') || '-tsan'
    WHEN (`t1`.`CiSanitizer` = 'memory') THEN
    coalesce(`t1`.`CiBuildType`, 'release') || '-msan'
    ELSE coalesce(`t1`.`CiBuildType`, 'release')
    END AS `res_3`, IF(String::Contains(`t1`.`CiJobTitle`, 'nemesis false'), 'nemesis off', 'nemesis on') AS `res_4`, '(a ' || '"' || Unicode::ReplaceAll(CAST(`t1`.`ReportUrl` AS UTF8), coalesce(CAST('"' AS UTF8), ''), coalesce(CAST('""' AS UTF8), '')) || '"' || ' "REPORT")' AS `res_5`, `t1`.`Timestamp` AS `res_6`, Unicode::ReplaceAll(CAST(Unicode::ReplaceAll(CAST(Unicode::ReplaceAll(CAST(`t1`.`Test` AS UTF8), coalesce(CAST('_nemesis_False' AS UTF8), ''), coalesce(CAST('' AS UTF8), '')) AS UTF8), coalesce(CAST('_nemesis_True' AS UTF8), ''), coalesce(CAST('' AS UTF8), '')) AS UTF8), coalesce(CAST('Workload' AS UTF8), ''), coalesce(CAST('' AS UTF8), '')) AS `res_7`, `t1`.`PlannedDuration` AS `res_8`, (`t1`.`TotalExecutionTime` / `t1`.`TotalThreads`) * `t1`.`TotalIterations` AS `res_9`
ORDER BY  `res_0` DESC, `res_1` ASC, `res_2` ASC, `res_3` ASC, `res_4` ASC, `res_5` ASC, `res_6` ASC, `res_7` ASC, `res_8` ASC, `res_9` ASC LIMIT 100001