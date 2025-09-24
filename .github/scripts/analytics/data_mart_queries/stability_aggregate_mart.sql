-- Stability Aggregate Data Mart
-- Разворачивание данных для записей с aggregation_level = "aggregate"

$run_id_limit = CAST(CurrentUtcTimestamp() AS Uint64) - 30UL * 86400UL * 1000000UL;

-- Находим Suite с _Verification записями (успешно стартовавшие тесты)
$verification_suites = SELECT
    Db,
    Suite,
    RunId,
    Timestamp AS VerificationTimestamp,
    Success AS VerificationSuccess,
    Info AS VerificationInfo
FROM `nemesis/tests_results`
WHERE 
    CAST(RunId AS Uint64) / 1000UL > $run_id_limit
    AND Kind = 'Load'
    AND Test = '_Verification';

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

$aggregate_data = SELECT
    v.Db AS Db,
    v.Suite AS Suite,
    COALESCE(s.Test, String::ReplaceAll(v.Suite, 'Workload', '') || 'Workload') AS Test,
    CASE WHEN s.Test IS NULL THEN 1U ELSE 0U END AS IsCrashed,
    v.RunId AS RunId,
    COALESCE(s.Timestamp, v.VerificationTimestamp) AS Timestamp,
    COALESCE(s.Success, 0U) AS Success,
    
    -- Извлекаем основные агрегированные метрики из Stats JSON (NULL если нет Stability записи)
    CAST(JSON_VALUE(s.Stats, '$.total_runs') AS Int32) AS TotalRuns,
    CAST(JSON_VALUE(s.Stats, '$.successful_runs') AS Int32) AS SuccessfulRuns,
    CAST(JSON_VALUE(s.Stats, '$.failed_runs') AS Int32) AS FailedRuns,
    CAST(JSON_VALUE(s.Stats, '$.total_iterations') AS Int32) AS TotalIterations,
    CAST(JSON_VALUE(s.Stats, '$.successful_iterations') AS Int32) AS SuccessfulIterations,
    CAST(JSON_VALUE(s.Stats, '$.failed_iterations') AS Int32) AS FailedIterations,
    
    -- Временные метрики
    CAST(JSON_VALUE(s.Stats, '$.planned_duration') AS Float) AS PlannedDuration,
    CAST(JSON_VALUE(s.Stats, '$.actual_duration') AS Float) AS ActualDuration,
    CAST(JSON_VALUE(s.Stats, '$.total_execution_time') AS Float) AS TotalExecutionTime,
    
    -- Метрики производительности
    CAST(JSON_VALUE(s.Stats, '$.success_rate') AS Float) AS SuccessRate,
    CAST(JSON_VALUE(s.Stats, '$.avg_threads_per_iteration') AS Int32) AS AvgThreadsPerIteration,
    CAST(JSON_VALUE(s.Stats, '$.total_threads') AS Int32) AS TotalThreads,
    CAST(JSON_VALUE(s.Stats, '$.nodes_percentage') AS Int32) AS NodesPercentage,
    CAST(JSON_VALUE(s.Stats, '$.nodes_with_issues') AS Int32) AS NodesWithIssues,
    
    -- Ошибки и диагностика
    JSON_VALUE(s.Stats, '$.node_error_messages') AS NodeErrorMessages, -- JSON массив с подробностями ошибок нод
    JSON_VALUE(s.Stats, '$.workload_error_messages') AS WorkloadErrorMessages, -- JSON массив с ошибками workload
    JSON_VALUE(s.Stats, '$.workload_warning_messages') AS WorkloadWarningMessages, -- JSON массив с предупреждениями workload
    
    -- Настройки теста
    CASE WHEN JSON_VALUE(s.Stats, '$.use_iterations') = 'true' THEN 1U ELSE 0U END AS UseIterations,
    CASE WHEN JSON_VALUE(s.Stats, '$.nemesis') = 'true' THEN 1U ELSE 0U END AS Nemesis,
    CASE WHEN JSON_VALUE(s.Stats, '$.nemesis_enabled') = 'true' THEN 1U ELSE 0U END AS NemesisEnabled,
    JSON_VALUE(s.Stats, '$.workload_type') AS WorkloadType,
    JSON_VALUE(s.Stats, '$.path_template') AS PathTemplate,
    
    -- Статус ошибок и предупреждений  
    CASE WHEN JSON_VALUE(s.Stats, '$.node_errors') = 'true' THEN 1U ELSE 0U END AS NodeErrors,
    CASE WHEN JSON_VALUE(s.Stats, '$.workload_errors') = 'true' THEN 1U ELSE 0U END AS WorkloadErrors,
    CASE WHEN JSON_VALUE(s.Stats, '$.workload_warnings') = 'true' THEN 1U ELSE 0U END AS WorkloadWarnings,
    
    -- Дополнительные поля
    JSON_VALUE(s.Stats, '$.table_type') AS TableType, -- для SimpleQueue workload
    
    -- Временная метка теста
    CAST(JSON_VALUE(s.Stats, '$.test_timestamp') AS Uint64) AS TestTimestamp,
    CAST(v.RunId AS Uint64) AS StatsRunId,  -- Используем RunId из verification вместо Stats
    
    -- Извлекаем информацию о кластере из Info JSON
    JSON_VALUE(COALESCE(s.Info, v.VerificationInfo), '$.cluster.version') AS ClusterVersion,
    -- Создаем ссылку на GitHub commit из версии кластера (main.108fc20 -> https://github.com/ydb-platform/ydb/commit/108fc20)
    CASE
        WHEN JSON_VALUE(COALESCE(s.Info, v.VerificationInfo), '$.cluster.version') IS NOT NULL THEN
            'https://github.com/ydb-platform/ydb/commit/' || String::SplitToList(JSON_VALUE(COALESCE(s.Info, v.VerificationInfo), '$.cluster.version'), '.')[1]
        ELSE NULL
    END AS ClusterVersionLink,
    JSON_VALUE(COALESCE(s.Info, v.VerificationInfo), '$.cluster.endpoint') AS ClusterEndpoint,
    JSON_VALUE(COALESCE(s.Info, v.VerificationInfo), '$.cluster.database') AS ClusterDatabase,
    -- Извлекаем мониторинг кластера из endpoint (@grpc://host:port/ -> host:monitoring_port)
        CASE
            WHEN JSON_VALUE(COALESCE(s.Info, v.VerificationInfo), '$.cluster.endpoint') IS NOT NULL THEN
                String::SplitToList(String::SplitToList(JSON_VALUE(COALESCE(s.Info, v.VerificationInfo), '$.cluster.endpoint'), '//')[1], ':')[0] || ':8765'
            ELSE NULL
        END AS ClusterMonitoring,
    CAST(JSON_VALUE(COALESCE(s.Info, v.VerificationInfo), '$.cluster.nodes_count') AS Int32) AS NodesCount,
    JSON_VALUE(COALESCE(s.Info, v.VerificationInfo), '$.cluster.nodes_info') AS NodesInfo, -- JSON массив с информацией о нодах
    JSON_VALUE(COALESCE(s.Info, v.VerificationInfo), '$.ci_version') AS CiVersion,
    JSON_VALUE(COALESCE(s.Info, v.VerificationInfo), '$.test_tools_version') AS TestToolsVersion,
    JSON_VALUE(COALESCE(s.Info, v.VerificationInfo), '$.report_url') AS ReportUrl,
    JSON_VALUE(COALESCE(s.Info, v.VerificationInfo), '$.ci_launch_id') AS CiLaunchId,
    JSON_VALUE(COALESCE(s.Info, v.VerificationInfo), '$.ci_launch_url') AS CiLaunchUrl,
    JSON_VALUE(COALESCE(s.Info, v.VerificationInfo), '$.ci_launch_start_time') AS CiLaunchStartTime,
    JSON_VALUE(COALESCE(s.Info, v.VerificationInfo), '$.ci_job_title') AS CiJobTitle,
    JSON_VALUE(COALESCE(s.Info, v.VerificationInfo), '$.ci_cluster_name') AS CiClusterName,
    
    -- Флаг того, что тест имел успешную верификацию 
    v.VerificationSuccess AS HadVerification
    
FROM $verification_suites AS v
LEFT JOIN $stability_suites AS s 
    ON v.Db = s.Db 
    AND v.Suite = s.Suite 
    AND v.RunId = s.RunId;

SELECT
    agg.Db,
    agg.Suite,
    agg.Test,
    CAST(CAST(agg.RunId AS Uint64)/1000 AS Timestamp) AS RunTs,
    agg.Timestamp,
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
    agg.HadVerification,
    
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
    
    -- Общий статус выполнения
    CASE
        WHEN agg.IsCrashed = 1U THEN 'crashed_during_execution'  -- Нет Stability записи
        WHEN agg.Success = 1U AND (agg.NodeErrors IS NULL OR agg.NodeErrors = 0U) AND (agg.WorkloadErrors IS NULL OR agg.WorkloadErrors = 0U) THEN 'success'
        WHEN agg.Success = 1U AND (agg.NodeErrors = 1U OR agg.WorkloadErrors = 1U) THEN 'success_with_errors'
        WHEN agg.Success = 0U AND agg.NodeErrors = 1U THEN 'node_failure'
        WHEN agg.Success = 0U AND agg.WorkloadErrors = 1U THEN 'workload_failure'
        WHEN agg.Success = 0U THEN 'failure'
        ELSE 'unknown'
    END AS OverallStatus

FROM $aggregate_data AS agg
ORDER BY RunTs DESC;
