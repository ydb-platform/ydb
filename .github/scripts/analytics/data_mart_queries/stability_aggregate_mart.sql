-- Stability Aggregate Data Mart
-- Разворачивание данных для записей с aggregation_level = "aggregate"

$run_id_limit = CAST(CurrentUtcTimestamp() AS Uint64) - 30UL * 86400UL * 1000000UL;

-- Находим записи проверок кластера (ClusterCheck)
$cluster_checks = SELECT
    Db,
    Suite,
    Test,
    RunId,
    Timestamp AS CheckTimestamp,
    Success AS CheckSuccess,
    Stats AS CheckStats,
    Info AS CheckInfo
FROM `nemesis/tests_results`
WHERE 
    CAST(RunId AS Uint64) / 1000UL > $run_id_limit
    AND Kind = 'ClusterCheck';

-- Находим Suite со Stability записями
$stability_suites = SELECT
    Db,
    Suite,
    Test,
    RunId,
    Timestamp,
    Success,
    Stats,
    Info
FROM `nemesis/tests_results`
WHERE 
    CAST(RunId AS Uint64) / 1000UL > $run_id_limit
    AND Kind = 'Stability'
    AND JSON_VALUE(Stats, '$.aggregation_level') = 'aggregate';

-- Объединяем ClusterCheck записи со Stability записями (LEFT JOIN от ClusterCheck к Stability)
$all_tests = SELECT
    cc.Db AS Db,
    cc.Suite AS Suite,
    cc.Test AS Test,
    cc.RunId AS RunId,
    s.Timestamp AS StabilityTimestamp,  -- Может быть NULL если нет Stability записи
    s.Success AS Success,               -- Может быть NULL если нет Stability записи
    
    -- Информация о проверке кластера (из ClusterCheck записи - всегда есть)
    cc.CheckSuccess AS ClusterCheckSuccess,
    cc.CheckTimestamp AS ClusterCheckTimestamp,
    JSON_VALUE(cc.CheckStats, '$.issue_type') AS ClusterIssueType,
    JSON_VALUE(cc.CheckStats, '$.issue_description') AS ClusterIssueDescription,
    JSON_VALUE(cc.CheckStats, '$.verification_phase') AS ClusterIssuePhase,
    JSON_VALUE(cc.CheckStats, '$.check_type') AS ClusterCheckType,
    CASE WHEN JSON_VALUE(cc.CheckStats, '$.is_critical') = 'true' THEN 1U ELSE 0U END AS ClusterIssueCritical,
    
    -- Извлекаем основные агрегированные метрики из Stats JSON (NULL для кластерных проблем)
    CAST(JSON_VALUE(s.Stats, '$.total_runs') AS Int32) AS TotalRuns,
    CAST(JSON_VALUE(s.Stats, '$.successful_runs') AS Int32) AS SuccessfulRuns,
    CAST(JSON_VALUE(s.Stats, '$.failed_runs') AS Int32) AS FailedRuns,
    CAST(JSON_VALUE(s.Stats, '$.total_iterations') AS Int32) AS TotalIterations,
    CAST(JSON_VALUE(s.Stats, '$.successful_iterations') AS Int32) AS SuccessfulIterations,
    CAST(JSON_VALUE(s.Stats, '$.failed_iterations') AS Int32) AS FailedIterations,
    
    -- Временные метрики (NULL для кластерных проблем)
    CAST(JSON_VALUE(s.Stats, '$.planned_duration') AS Float) AS PlannedDuration,
    CAST(JSON_VALUE(s.Stats, '$.actual_duration') AS Float) AS ActualDuration,
    CAST(JSON_VALUE(s.Stats, '$.total_execution_time') AS Float) AS TotalExecutionTime,
    
    -- Метрики производительности (NULL для кластерных проблем)
    CAST(JSON_VALUE(s.Stats, '$.success_rate') AS Float) AS SuccessRate,
    CAST(JSON_VALUE(s.Stats, '$.avg_threads_per_iteration') AS Int32) AS AvgThreadsPerIteration,
    CAST(JSON_VALUE(s.Stats, '$.total_threads') AS Int32) AS TotalThreads,
    CAST(JSON_VALUE(s.Stats, '$.nodes_percentage') AS Int32) AS NodesPercentage,
    CAST(JSON_VALUE(s.Stats, '$.nodes_with_issues') AS Int32) AS NodesWithIssues,
    
    -- Ошибки и диагностика (NULL для кластерных проблем)
    JSON_QUERY(s.Stats, '$.node_error_messages') AS NodeErrorMessages,
    JSON_QUERY(s.Stats, '$.workload_error_messages') AS WorkloadErrorMessages,
    JSON_QUERY(s.Stats, '$.workload_warning_messages') AS WorkloadWarningMessages,
    
    -- Настройки теста (NULL для кластерных проблем)
    CASE WHEN JSON_VALUE(s.Stats, '$.use_iterations') = 'true' THEN 1U ELSE 0U END AS UseIterations,
    CASE WHEN JSON_VALUE(s.Stats, '$.nemesis') = 'true' THEN 1U ELSE 0U END AS Nemesis,
    CASE WHEN JSON_VALUE(s.Stats, '$.nemesis_enabled') = 'true' THEN 1U ELSE 0U END AS NemesisEnabled,
    JSON_VALUE(s.Stats, '$.workload_type') AS WorkloadType,
    JSON_VALUE(s.Stats, '$.path_template') AS PathTemplate,
    
    -- Статус ошибок и предупреждений (NULL для кластерных проблем)
    CASE WHEN JSON_VALUE(s.Stats, '$.node_errors') = 'true' THEN 1U ELSE 0U END AS NodeErrors,
    CASE WHEN JSON_VALUE(s.Stats, '$.workload_errors') = 'true' THEN 1U ELSE 0U END AS WorkloadErrors,
    CASE WHEN JSON_VALUE(s.Stats, '$.workload_warnings') = 'true' THEN 1U ELSE 0U END AS WorkloadWarnings,
    
    -- Дополнительные поля
    s.Stats AS Stats,  -- Добавляем Stats для использования в финальном SELECT
    JSON_VALUE(s.Stats, '$.table_type') AS TableType,
    CAST(JSON_VALUE(s.Stats, '$.test_timestamp') AS Uint64) AS TestTimestamp,
    CAST(s.RunId AS Uint64) AS StatsRunId,
    
    -- Извлекаем информацию о кластере из Info JSON
    JSON_VALUE(COALESCE(s.Info, cc.CheckInfo), '$.cluster.version') AS ClusterVersion,
    CASE
        WHEN JSON_VALUE(COALESCE(s.Info, cc.CheckInfo), '$.cluster.version') IS NOT NULL THEN
            'https://github.com/ydb-platform/ydb/commit/' || String::SplitToList(JSON_VALUE(COALESCE(s.Info, cc.CheckInfo), '$.cluster.version'), '.')[1]
        ELSE NULL
    END AS ClusterVersionLink,
    JSON_VALUE(s.Info, '$.cluster.endpoint') AS ClusterEndpoint,
    JSON_VALUE(s.Info, '$.cluster.database') AS ClusterDatabase,
    CASE
        WHEN JSON_VALUE(s.Info, '$.cluster.endpoint') IS NOT NULL THEN
            String::SplitToList(String::SplitToList(JSON_VALUE(s.Info, '$.cluster.endpoint'), '//')[1], ':')[0] || ':8765/'
        ELSE NULL
    END AS ClusterMonitoring,
    CAST(JSON_VALUE(s.Info, '$.cluster.nodes_count') AS Int32) AS NodesCount,
    JSON_QUERY(s.Info, '$.cluster.nodes_info') AS NodesInfo,
    JSON_VALUE(s.Info, '$.ci_version') AS CiVersion,
    JSON_VALUE(s.Info, '$.test_tools_version') AS TestToolsVersion,
    JSON_VALUE(s.Info, '$.report_url') AS ReportUrl,
    JSON_VALUE(s.Info, '$.ci_launch_id') AS CiLaunchId,
    JSON_VALUE(s.Info, '$.ci_launch_url') AS CiLaunchUrl,
    JSON_VALUE(s.Info, '$.ci_launch_start_time') AS CiLaunchStartTime,
    JSON_VALUE(s.Info, '$.ci_job_title') AS CiJobTitle,
    JSON_VALUE(s.Info, '$.ci_cluster_name') AS CiClusterName,
    JSON_VALUE(COALESCE(s.Info, cc.CheckInfo), '$.ci_nemesis') AS CiNemesis,
    JSON_VALUE(COALESCE(s.Info, cc.CheckInfo), '$.ci_build_type') AS CiBuildType,
    JSON_VALUE(COALESCE(s.Info, cc.CheckInfo), '$.ci_sanitizer') AS CiSanitizer,
    
    -- Порядок выполнения
    ROW_NUMBER() OVER (PARTITION BY cc.RunId ORDER BY COALESCE(s.Timestamp, cc.CheckTimestamp), cc.Suite, cc.Test) AS OrderInRun
    
FROM $cluster_checks AS cc
LEFT JOIN $stability_suites AS s
    ON cc.Db = s.Db 
    AND cc.RunId = s.RunId
    AND cc.Suite = s.Suite
    AND cc.Test = s.Test  -- Матчим по одинаковому Test имени!
WHERE s.Stats IS NULL OR JSON_VALUE(s.Stats, '$.aggregation_level') = 'aggregate';


SELECT
    agg.Db,
    agg.Suite,
    agg.Test,
    CAST(CAST(agg.RunId AS Uint64)/1000UL AS Timestamp) AS RunTs,
    agg.StabilityTimestamp AS Timestamp,
    
    -- Четкие поля времени старта и окончания
    agg.ClusterCheckTimestamp AS StartTime,  -- Время старта проверки кластера
    agg.StabilityTimestamp AS EndTime,       -- Время выгрузки результатов (окончание всего теста)
    
    -- Времена workload из Stats
    CAST(JSON_VALUE(agg.Stats, '$.workload_start_time') AS Timestamp) AS WorkloadStartTime,
    CAST(JSON_VALUE(agg.Stats, '$.workload_end_time') AS Timestamp) AS WorkloadEndTime,
    CAST(JSON_VALUE(agg.Stats, '$.workload_duration') AS Float) AS WorkloadDurationSeconds,
    
    -- Общая длительность теста (от ClusterCheck до выгрузки)
    CASE 
        WHEN agg.StabilityTimestamp IS NOT NULL AND agg.ClusterCheckTimestamp IS NOT NULL THEN
            CAST(agg.StabilityTimestamp - agg.ClusterCheckTimestamp AS Float) / 1000000.0  -- микросекунды в секунды
        ELSE NULL
    END AS TotalTestDurationSeconds,
    agg.Success,
    
    -- Основные метрики
    agg.TotalRuns,
    agg.SuccessfulRuns,
    agg.FailedRuns,
    agg.TotalIterations,
    agg.SuccessfulIterations,
    agg.FailedIterations,
    
    -- Временные метрики
    agg.PlannedDuration,
    agg.ActualDuration,
    agg.TotalExecutionTime,
    
    -- Производительность
    agg.SuccessRate,
    agg.AvgThreadsPerIteration,
    agg.TotalThreads,
    agg.NodesPercentage,
    agg.NodesWithIssues,
    agg.NodeErrorMessages,
    agg.WorkloadErrorMessages,
    agg.WorkloadWarningMessages,
    
    -- Настройки
    agg.UseIterations,
    agg.Nemesis,
    agg.NemesisEnabled,
    agg.WorkloadType,
    agg.PathTemplate,
    
    -- Статусы ошибок
    agg.NodeErrors,
    agg.WorkloadErrors,
    agg.WorkloadWarnings,
    
    -- Дополнительные поля
    agg.TableType,
    
    -- Временные метки
    agg.TestTimestamp,
    agg.StatsRunId,
    
    -- Кластерные проверки и проблемы
    agg.ClusterCheckSuccess,
    agg.ClusterIssueType,
    agg.ClusterIssueDescription,
    agg.ClusterIssuePhase,
    agg.ClusterCheckType,
    agg.ClusterIssueCritical,
    
    -- Информация о кластере
    agg.ClusterVersion,
    agg.ClusterVersionLink,
    agg.ClusterEndpoint,
    agg.ClusterDatabase,
    agg.ClusterMonitoring,
    agg.NodesCount,
    agg.NodesInfo,
    agg.CiVersion,
    agg.TestToolsVersion,
    agg.ReportUrl,
    agg.CiLaunchId,
    agg.CiLaunchUrl,
    agg.CiLaunchStartTime,
    agg.CiJobTitle,
    agg.CiClusterName,
    agg.CiNemesis,
    agg.CiBuildType,
    agg.CiSanitizer,
    agg.OrderInRun,
    
    -- Извлекаем ветки из версий
    COALESCE(SubString(CAST(agg.ClusterVersion AS String), 0U, FIND(CAST(agg.ClusterVersion AS String), '.')), 'unknown') AS Branch,
    COALESCE(SubString(CAST(agg.CiVersion AS String), 0U, FIND(CAST(agg.CiVersion AS String), '.')), 'unknown') AS CiBranch,
    COALESCE(SubString(CAST(agg.TestToolsVersion AS String), 0U, FIND(CAST(agg.TestToolsVersion AS String), '.')), 'unknown') AS TestToolsBranch,
    
    -- Вычисляемые метрики
    CASE 
        WHEN agg.TotalRuns > 0 THEN CAST(agg.FailedRuns AS Float) / CAST(agg.TotalRuns AS Float)
        ELSE 0.0
    END AS FailureRate,
    
    CASE 
        WHEN agg.TotalIterations > 0 THEN CAST(agg.FailedIterations AS Float) / CAST(agg.TotalIterations AS Float)
        ELSE 0.0
    END AS IterationFailureRate,
    
    CASE 
        WHEN agg.PlannedDuration > 0 THEN agg.ActualDuration / agg.PlannedDuration
        ELSE NULL
    END AS DurationRatio,
    
    -- Типы найденных ошибок нод (анализ node_error_messages)
    CASE
        WHEN agg.NodeErrorMessages IS NULL OR CAST(agg.NodeErrorMessages AS String) = '[]' OR CAST(agg.NodeErrorMessages AS String) = '' THEN NULL
        ELSE
            ListConcat(
                ListUniq(
                    ListNotNull(AsList(
                        CASE WHEN CAST(agg.NodeErrorMessages AS String) LIKE '%coredump%' THEN 'Coredump' ELSE NULL END,
                        CASE WHEN CAST(agg.NodeErrorMessages AS String) LIKE '%OOM%' OR CAST(agg.NodeErrorMessages AS String) LIKE '%experienced OOM%' THEN 'OOM' ELSE NULL END,
                        CASE WHEN CAST(agg.NodeErrorMessages AS String) LIKE '%VERIFY%' OR CAST(agg.NodeErrorMessages AS String) LIKE '%verify%' THEN 'Verify' ELSE NULL END,
                        CASE WHEN CAST(agg.NodeErrorMessages AS String) LIKE '%SAN%' OR CAST(agg.NodeErrorMessages AS String) LIKE '%sanitizer%' THEN 'SAN' ELSE NULL END
                    ))
                ),
                ', '
            )
    END AS FacedNodeErrors,
    
    -- Общий статус выполнения с детализацией кластерных проблем
    CASE
        -- Приоритет 1: Проблемы с инфраструктурой (ClusterCheck failed)
        WHEN agg.ClusterCheckSuccess = 0U THEN 'infrastructure_error'
        
        -- Приоритет 2: Кластер OK, но нет Stability записи (тест завис/не завершился)
        WHEN agg.ClusterCheckSuccess = 1U AND agg.Success IS NULL THEN 
            CASE 
                -- Если прошло больше чем planned_duration * 3 от времени старта ClusterCheck, считаем timeout
                -- Сравниваем время в микросекундах: текущее время - время старта > planned_duration * 3 * 1000000
                WHEN CAST(CurrentUtcTimestamp() AS Uint64) - CAST(agg.ClusterCheckTimestamp AS Uint64) > CAST(COALESCE(agg.PlannedDuration, 1800) * 3 * 1000000 AS Uint64) THEN 'timeout'
                ELSE 'in_progress'
            END
        
        -- Приоритет 3: Обычные workload тесты (если кластер OK и есть Stability запись)
        WHEN agg.Success = 1U AND (agg.NodeErrors IS NULL OR agg.NodeErrors = 0U) AND (agg.WorkloadErrors IS NULL OR agg.WorkloadErrors = 0U) THEN 'success'
        WHEN agg.Success = 1U AND (agg.NodeErrors = 1U OR agg.WorkloadErrors = 1U) THEN 'success_with_errors'
        WHEN agg.Success = 0U AND agg.NodeErrors = 1U THEN 'node_failure'
        WHEN agg.Success = 0U AND agg.WorkloadErrors = 1U THEN 'success' -- 'workload_failure'
        WHEN agg.Success = 0U THEN 'failure'
        ELSE 'unknown'
    END AS OverallStatus

FROM $all_tests AS agg
ORDER BY RunTs DESC;
