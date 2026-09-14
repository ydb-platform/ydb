-- https://datalens.yandex/ug26hdjlxoeoi
SELECT `res_0`,
         `res_1`,
         `res_2`,
         `res_3`,
        
    CASE
    WHEN (max(CASE
    WHEN (`t1`.`Branch` = 'main') THEN
    CASE
    WHEN (`t1`.`Suite` = 'Tpch10000') THEN
    CASE
    WHEN (`t1`.`Test` = 'Query17') THEN
    'https://github.com/ydb-platform/ydb/issues/16849'
    WHEN (`t1`.`Test` = 'Query18') THEN
    'https://github.com/ydb-platform/ydb/issues/17633'
    ELSE NULL
    END
    WHEN (`t1`.`Suite` = 'Tpcds1000'
        AND `t1`.`Test` = 'Query14') THEN
    'https://github.com/ydb-platform/ydb/issues/17495'
    ELSE NULL
    END
    WHEN String::StartsWith(`t1`.`Branch`, 'stable') THEN
    CASE
    WHEN (`t1`.`Suite` = 'Tpcds1000') THEN
    CASE
    WHEN (`t1`.`Test` IN ('Query05', 'Query14')) THEN
    'https://github.com/ydb-platform/ydb/issues/17495'
    WHEN (`t1`.`Test` IN ('Query08', 'Query67')) THEN
    'https://github.com/ydb-platform/ydb/issues/15359'
    ELSE NULL
    END
    WHEN (`t1`.`Suite` = 'Tpch10000') THEN
    CASE
    WHEN (`t1`.`Test` = 'Query17') THEN
    'https://github.com/ydb-platform/ydb/issues/16849'
    WHEN (`t1`.`Test` IN ('Query07', 'Query09', 'Query10', 'Query12', 'Query18', 'Query19')) THEN
    'https://github.com/ydb-platform/ydb/issues/17523'
    ELSE NULL END
    ELSE NULL END
    ELSE NULL END) != '') THEN
    '(c ' || '"' || Unicode::ReplaceAll(CAST(IF(min(`t1`.`Run_number_in_version`) = 1
        AND COUNT_IF(`t1`.`Timestamp` IS NULL) > 0, IF(COUNT_IF(`t1`.`Timestamp` IS NULL) = 1, 'in progress', 'in progress: ' || CAST(COUNT_IF(`t1`.`Timestamp` IS NULL) AS UTF8)),
    CASE
    WHEN (max(`t1`.`Report`) IS NULL) THEN
    'No run'
    WHEN (sum(`t1`.`diff_response`) > 0) THEN
    IF(COUNT_IF(`t1`.`Success` = 0) = 0
        AND count(`t1`.`Test`) = 1, 'diff: ' || CAST(sum(`t1`.`diff_response`) AS UTF8) || 'q; ' || CAST(CAST(sum(coalesce(`t1`.`YdbSumMeans`, 0)) AS DOUBLE) / 1000 AS UTF8) || ' s', IF(COUNT_IF(`t1`.`Success` = 0) = 1
        AND count(`t1`.`Test`) = 1, 'err+diff: ' || CAST(sum(`t1`.`diff_response`) AS UTF8) || 'q; ' || CAST(CAST(sum(coalesce(`t1`.`YdbSumMeans`, 0)) AS DOUBLE) / 1000 AS UTF8) || ' s', 'err:' || CAST(COUNT_IF(`t1`.`Success` = 0) AS UTF8) || ' diff:' || CAST(COUNT_IF(`t1`.`diff_response` > 0) AS UTF8) || '; ' || CAST(CAST(sum(coalesce(`t1`.`YdbSumMeans`, 0)) AS DOUBLE) / 1000 AS UTF8) || ' s'))
    WHEN (COUNT_IF(`t1`.`Success` = 0) > 0) THEN
    CASE
    WHEN (COUNT_IF(`t1`.`Timestamp` IS NULL) = 1) THEN
    'not executed'
    WHEN (COUNT_IF(`t1`.`Success` = 0) = 1
        AND count(`t1`.`Test`) = 1) THEN
    'err: ' || CAST(CAST(sum(coalesce(`t1`.`YdbSumMeans`, 0)) AS DOUBLE) / 1000 AS UTF8) || ' s'
    ELSE 'err:' || CAST(COUNT_IF(`t1`.`Success` = 0) AS UTF8) || '; ' || CAST(CAST(sum(coalesce(`t1`.`YdbSumMeans`, 0)) AS DOUBLE) / 1000 AS UTF8) || ' s'
    END
    WHEN (max(`t1`.`Test`) = '_Verification'
        AND max(`t1`.`Success`) = 1) THEN
    CAST(CAST(sum(coalesce(`t1`.`YdbSumMeans`, 0)) AS DOUBLE) / 1000 AS UTF8) || ' s'
    WHEN (sum(`t1`.`YdbSumMeans`) IS NOT NULL) THEN
    CAST(CAST(sum(coalesce(`t1`.`YdbSumMeans`, 0)) AS DOUBLE) / 1000 AS UTF8) || ' s'
    ELSE NULL END) AS UTF8), coalesce(CAST('"' AS UTF8), ''), coalesce(CAST('""' AS UTF8), '')) || '"' || ' ' || '(a ' || '"' || Unicode::ReplaceAll(CAST(max(CASE
    WHEN (`t1`.`Branch` = 'main') THEN
    CASE
    WHEN (`t1`.`Suite` = 'Tpch10000') THEN
    CASE
    WHEN (`t1`.`Test` = 'Query17') THEN
    'https://github.com/ydb-platform/ydb/issues/16849'
    WHEN (`t1`.`Test` = 'Query18') THEN
    'https://github.com/ydb-platform/ydb/issues/17633'
    ELSE NULL
    END
    WHEN (`t1`.`Suite` = 'Tpcds1000'
        AND `t1`.`Test` = 'Query14') THEN
    'https://github.com/ydb-platform/ydb/issues/17495'
    ELSE NULL
    END
    WHEN String::StartsWith(`t1`.`Branch`, 'stable') THEN
    CASE
    WHEN (`t1`.`Suite` = 'Tpcds1000') THEN
    CASE
    WHEN (`t1`.`Test` IN ('Query05', 'Query14')) THEN
    'https://github.com/ydb-platform/ydb/issues/17495'
    WHEN (`t1`.`Test` IN ('Query08', 'Query67')) THEN
    'https://github.com/ydb-platform/ydb/issues/15359'
    ELSE NULL
    END
    WHEN (`t1`.`Suite` = 'Tpch10000') THEN
    CASE
    WHEN (`t1`.`Test` = 'Query17') THEN
    'https://github.com/ydb-platform/ydb/issues/16849'
    WHEN (`t1`.`Test` IN ('Query07', 'Query09', 'Query10', 'Query12', 'Query18', 'Query19')) THEN
    'https://github.com/ydb-platform/ydb/issues/17523'
    ELSE NULL END
    ELSE NULL END
    ELSE NULL END) AS UTF8), coalesce(CAST('"' AS UTF8), ''), coalesce(CAST('""' AS UTF8), '')) || '"' || ' ' || '"' || Unicode::ReplaceAll(CAST(' issue ' || Unicode::ReplaceAll(CAST(max(CASE
    WHEN (`t1`.`Branch` = 'main') THEN
    CASE
    WHEN (`t1`.`Suite` = 'Tpch10000') THEN
    CASE
    WHEN (`t1`.`Test` = 'Query17') THEN
    'https://github.com/ydb-platform/ydb/issues/16849'
    WHEN (`t1`.`Test` = 'Query18') THEN
    'https://github.com/ydb-platform/ydb/issues/17633'
    ELSE NULL
    END
    WHEN (`t1`.`Suite` = 'Tpcds1000'
        AND `t1`.`Test` = 'Query14') THEN
    'https://github.com/ydb-platform/ydb/issues/17495'
    ELSE NULL
    END
    WHEN String::StartsWith(`t1`.`Branch`, 'stable') THEN
    CASE
    WHEN (`t1`.`Suite` = 'Tpcds1000') THEN
    CASE
    WHEN (`t1`.`Test` IN ('Query05', 'Query14')) THEN
    'https://github.com/ydb-platform/ydb/issues/17495'
    WHEN (`t1`.`Test` IN ('Query08', 'Query67')) THEN
    'https://github.com/ydb-platform/ydb/issues/15359'
    ELSE NULL
    END
    WHEN (`t1`.`Suite` = 'Tpch10000') THEN
    CASE
    WHEN (`t1`.`Test` = 'Query17') THEN
    'https://github.com/ydb-platform/ydb/issues/16849'
    WHEN (`t1`.`Test` IN ('Query07', 'Query09', 'Query10', 'Query12', 'Query18', 'Query19')) THEN
    'https://github.com/ydb-platform/ydb/issues/17523'
    ELSE NULL END
    ELSE NULL END
    ELSE NULL END) AS UTF8), coalesce(CAST('https://github.com/ydb-platform/ydb/issues/' AS UTF8), ''), coalesce(CAST('' AS UTF8), '')) AS UTF8), coalesce(CAST('"' AS UTF8), ''), coalesce(CAST('""' AS UTF8), '')) || '"' || ')' || ')'
    WHEN (max(`t1`.`Test`) = '_Verification') THEN
    '(c ' || '"' || Unicode::ReplaceAll(CAST(IF(min(`t1`.`Run_number_in_version`) = 1
        AND COUNT_IF(`t1`.`Timestamp` IS NULL) > 0, IF(COUNT_IF(`t1`.`Timestamp` IS NULL) = 1, 'in progress', 'in progress: ' || CAST(COUNT_IF(`t1`.`Timestamp` IS NULL) AS UTF8)),
    CASE
    WHEN (max(`t1`.`Report`) IS NULL) THEN
    'No run'
    WHEN (sum(`t1`.`diff_response`) > 0) THEN
    IF(COUNT_IF(`t1`.`Success` = 0) = 0
        AND count(`t1`.`Test`) = 1, 'diff: ' || CAST(sum(`t1`.`diff_response`) AS UTF8) || 'q; ' || CAST(CAST(sum(coalesce(`t1`.`YdbSumMeans`, 0)) AS DOUBLE) / 1000 AS UTF8) || ' s', IF(COUNT_IF(`t1`.`Success` = 0) = 1
        AND count(`t1`.`Test`) = 1, 'err+diff: ' || CAST(sum(`t1`.`diff_response`) AS UTF8) || 'q; ' || CAST(CAST(sum(coalesce(`t1`.`YdbSumMeans`, 0)) AS DOUBLE) / 1000 AS UTF8) || ' s', 'err:' || CAST(COUNT_IF(`t1`.`Success` = 0) AS UTF8) || ' diff:' || CAST(COUNT_IF(`t1`.`diff_response` > 0) AS UTF8) || '; ' || CAST(CAST(sum(coalesce(`t1`.`YdbSumMeans`, 0)) AS DOUBLE) / 1000 AS UTF8) || ' s'))
    WHEN (COUNT_IF(`t1`.`Success` = 0) > 0) THEN
    CASE
    WHEN (COUNT_IF(`t1`.`Timestamp` IS NULL) = 1) THEN
    'not executed'
    WHEN (COUNT_IF(`t1`.`Success` = 0) = 1
        AND count(`t1`.`Test`) = 1) THEN
    'err: ' || CAST(CAST(sum(coalesce(`t1`.`YdbSumMeans`, 0)) AS DOUBLE) / 1000 AS UTF8) || ' s'
    ELSE 'err:' || CAST(COUNT_IF(`t1`.`Success` = 0) AS UTF8) || '; ' || CAST(CAST(sum(coalesce(`t1`.`YdbSumMeans`, 0)) AS DOUBLE) / 1000 AS UTF8) || ' s'
    END
    WHEN (max(`t1`.`Test`) = '_Verification'
        AND max(`t1`.`Success`) = 1) THEN
    CAST(CAST(sum(coalesce(`t1`.`YdbSumMeans`, 0)) AS DOUBLE) / 1000 AS UTF8) || ' s'
    WHEN (sum(`t1`.`YdbSumMeans`) IS NOT NULL) THEN
    CAST(CAST(sum(coalesce(`t1`.`YdbSumMeans`, 0)) AS DOUBLE) / 1000 AS UTF8) || ' s'
    ELSE NULL END) || ': ' AS UTF8), coalesce(CAST('"' AS UTF8), ''), coalesce(CAST('""' AS UTF8), '')) || '"' || ' ' || '(a ' || '"' || Unicode::ReplaceAll(CAST(max(`t1`.`Report`) AS UTF8), coalesce(CAST('"' AS UTF8), ''), coalesce(CAST('""' AS UTF8), '')) || '"' || ' "report")' || ')'
    ELSE '(c ' || '"' || Unicode::ReplaceAll(CAST(IF(min(`t1`.`Run_number_in_version`) = 1
        AND COUNT_IF(`t1`.`Timestamp` IS NULL) > 0, IF(COUNT_IF(`t1`.`Timestamp` IS NULL) = 1, 'in progress', 'in progress: ' || CAST(COUNT_IF(`t1`.`Timestamp` IS NULL) AS UTF8)),
    CASE
    WHEN (max(`t1`.`Report`) IS NULL) THEN
    'No run'
    WHEN (sum(`t1`.`diff_response`) > 0) THEN
    IF(COUNT_IF(`t1`.`Success` = 0) = 0
        AND count(`t1`.`Test`) = 1, 'diff: ' || CAST(sum(`t1`.`diff_response`) AS UTF8) || 'q; ' || CAST(CAST(sum(coalesce(`t1`.`YdbSumMeans`, 0)) AS DOUBLE) / 1000 AS UTF8) || ' s', IF(COUNT_IF(`t1`.`Success` = 0) = 1
        AND count(`t1`.`Test`) = 1, 'err+diff: ' || CAST(sum(`t1`.`diff_response`) AS UTF8) || 'q; ' || CAST(CAST(sum(coalesce(`t1`.`YdbSumMeans`, 0)) AS DOUBLE) / 1000 AS UTF8) || ' s', 'err:' || CAST(COUNT_IF(`t1`.`Success` = 0) AS UTF8) || ' diff:' || CAST(COUNT_IF(`t1`.`diff_response` > 0) AS UTF8) || '; ' || CAST(CAST(sum(coalesce(`t1`.`YdbSumMeans`, 0)) AS DOUBLE) / 1000 AS UTF8) || ' s'))
    WHEN (COUNT_IF(`t1`.`Success` = 0) > 0) THEN
    CASE
    WHEN (COUNT_IF(`t1`.`Timestamp` IS NULL) = 1) THEN
    'not executed'
    WHEN (COUNT_IF(`t1`.`Success` = 0) = 1
        AND count(`t1`.`Test`) = 1) THEN
    'err: ' || CAST(CAST(sum(coalesce(`t1`.`YdbSumMeans`, 0)) AS DOUBLE) / 1000 AS UTF8) || ' s'
    ELSE 'err:' || CAST(COUNT_IF(`t1`.`Success` = 0) AS UTF8) || '; ' || CAST(CAST(sum(coalesce(`t1`.`YdbSumMeans`, 0)) AS DOUBLE) / 1000 AS UTF8) || ' s'
    END
    WHEN (max(`t1`.`Test`) = '_Verification'
        AND max(`t1`.`Success`) = 1) THEN
    CAST(CAST(sum(coalesce(`t1`.`YdbSumMeans`, 0)) AS DOUBLE) / 1000 AS UTF8) || ' s'
    WHEN (sum(`t1`.`YdbSumMeans`) IS NOT NULL) THEN
    CAST(CAST(sum(coalesce(`t1`.`YdbSumMeans`, 0)) AS DOUBLE) / 1000 AS UTF8) || ' s'
    ELSE NULL END) AS UTF8), coalesce(CAST('"' AS UTF8), ''), coalesce(CAST('""' AS UTF8), '')) || '"' || ' ' || '(a "" "")' || ')'
    END AS `res_4`, IF(COUNT_IF(`t1`.`Timestamp` IS NULL) = 0,
    CASE
    WHEN (COUNT_IF(`t1`.`Color` = 'red') > 0) THEN
    0
    WHEN (COUNT_IF(`t1`.`Color` = 'blue') > 0) THEN
    0.05
    WHEN (COUNT_IF(`t1`.`Color` = 'yellow') > 0) THEN
    0.1
    WHEN (COUNT_IF(`t1`.`Color` = 'green') > 0) THEN
    0.2
    ELSE 0 END, IF(max(`t1`.`Success`) = 0
        OR coalesce(sum(`t1`.`diff_response`), 0) > 0, 0.1, max(`t1`.`Success`))) AS `res_5`
FROM `perfomance/olap/fast_results` AS `t1`
WHERE `t1`.`CiBranch` IN ('unknown', 'trunk')
        AND `t1`.`TestToolsBranch` IN ('main', 'unknown')
        AND `t1`.`Branch` IN ('main')
        AND `t1`.`RunTs`
    BETWEEN (CurrentUtcTimestamp() - Interval('P3D'))
        AND CurrentUtcTimestamp()
GROUP BY  Unicode::ReplaceAll(CAST(Unicode::ReplaceAll(CAST(CAST(DateTime::MakeDatetime(DateTime::StartOf(`t1`.`Run_start_timestamp`, DateTime::IntervalFromDays(1))) AS DATE) AS UTF8), coalesce(CAST('"' AS UTF8), ''), coalesce(CAST('""' AS UTF8), '')) AS UTF8), coalesce(CAST('"' AS UTF8), ''), coalesce(CAST('""' AS UTF8), '')) AS `res_0`, `t1`.`Db` AS `res_1`, `t1`.`Suite` AS `res_2`, `t1`.`Test` AS `res_3`
ORDER BY  `res_0` DESC,
         `res_1` ASC,
         `res_2` ASC,
         `res_3` ASC LIMIT 10000 OFFSET 0