-- Workload Results Data Mart
-- Упрощенная архитектура: берем последний статус каждого теста

$run_id_limit = CAST(CurrentUtcTimestamp() AS Uint64) - 30UL * 86400UL * 1000000UL;

-- Находим последний статус каждого теста
$last_test_status = SELECT
    Db,
    Suite,
    Test,
    RunId,
    Kind,
    Timestamp,
    Success,
    Stats,
    Info,
    ROW_NUMBER() OVER (PARTITION BY Db, Suite, Test, RunId ORDER BY Timestamp DESC) AS rn
FROM `nemesis/tests_results`
WHERE 
    CAST(RunId AS Uint64) / 1000UL > $run_id_limit
    AND (
        Kind = 'TestInit'
        OR (Kind = 'ClusterCheck' AND Success = 0)  -- Только неуспешные проверки кластера
        OR (Kind = 'Stability' AND JSON_VALUE(Stats, '$.aggregation_level') = 'aggregate')
    );

-- Берем только последний статус для каждого теста
$latest_status = SELECT
    Db,
    Suite,
    Test,
    RunId,
    Kind AS LastKind,
    Timestamp AS LastTimestamp,
    Success AS LastSuccess,
    Stats AS LastStats,
    Info AS LastInfo
FROM $last_test_status
WHERE rn = 1;

-- Определяем статус теста на основе последнего события
$test_status = SELECT
    Db,
    Suite,
    Test,
    RunId,
    LastKind,
    LastTimestamp,
    LastSuccess,
    LastStats,
    LastInfo,
    
    -- Определяем общий статус на основе последнего события
    CASE
        WHEN LastKind = 'TestInit' THEN 
            CASE 
                -- Получаем планируемое время выполнения из TestInit (если есть)
                WHEN JSON_VALUE(LastStats, '$.planned_duration') IS NOT NULL THEN
                    CASE 
                        -- Если прошло больше planned_duration * 3, считаем timeout
                        WHEN CAST(CurrentUtcTimestamp() AS Uint64) - CAST(LastTimestamp AS Uint64) > CAST(JSON_VALUE(LastStats, '$.planned_duration') AS Float) * 3 * 1000000UL THEN 'timeout'
                        ELSE 'in_progress'
                    END
                -- Если нет planned_duration, используем дефолтное время (3 часа)
                WHEN CAST(CurrentUtcTimestamp() AS Uint64) - CAST(LastTimestamp AS Uint64) > 3UL * 3600UL * 1000000UL THEN 'broken'
                ELSE 'in_progress'
            END
        WHEN LastKind = 'ClusterCheck' THEN 'infrastructure_error'  -- Проблема с кластером
        WHEN LastKind = 'Stability' THEN 
            CASE
                WHEN LastSuccess = 1U AND (JSON_VALUE(LastStats, '$.node_errors') = 'true' OR JSON_VALUE(LastStats, '$.workload_errors') = 'true') THEN 'success_with_errors'
                WHEN LastSuccess = 1U THEN 'success'
                WHEN JSON_VALUE(LastStats, '$.node_errors') = 'true' THEN 'node_failure'
                WHEN JSON_VALUE(LastStats, '$.workload_errors') = 'true' THEN 'success'--'workload_failure'
                ELSE 'failure'
            END
        ELSE 'unknown'
    END AS OverallStatus,
    
    -- Извлекаем метрики только для Stability записей
    CASE WHEN LastKind = 'Stability' THEN CAST(JSON_VALUE(LastStats, '$.total_runs') AS Int32) ELSE NULL END AS TotalRuns,
    CASE WHEN LastKind = 'Stability' THEN CAST(JSON_VALUE(LastStats, '$.successful_runs') AS Int32) ELSE NULL END AS SuccessfulRuns,
    CASE WHEN LastKind = 'Stability' THEN CAST(JSON_VALUE(LastStats, '$.failed_runs') AS Int32) ELSE NULL END AS FailedRuns,
    CASE WHEN LastKind = 'Stability' THEN CAST(JSON_VALUE(LastStats, '$.total_iterations') AS Int32) ELSE NULL END AS TotalIterations,
    CASE WHEN LastKind = 'Stability' THEN CAST(JSON_VALUE(LastStats, '$.successful_iterations') AS Int32) ELSE NULL END AS SuccessfulIterations,
    CASE WHEN LastKind = 'Stability' THEN CAST(JSON_VALUE(LastStats, '$.failed_iterations') AS Int32) ELSE NULL END AS FailedIterations,
    
    -- Временные метрики
    CASE WHEN LastKind = 'Stability' THEN CAST(JSON_VALUE(LastStats, '$.planned_duration') AS Float) ELSE NULL END AS PlannedDuration,
    CASE WHEN LastKind = 'Stability' THEN CAST(JSON_VALUE(LastStats, '$.actual_duration') AS Float) ELSE NULL END AS ActualDuration,
    CASE WHEN LastKind = 'Stability' THEN CAST(JSON_VALUE(LastStats, '$.total_execution_time') AS Float) ELSE NULL END AS TotalExecutionTime,
    
    -- Метрики производительности
    CASE WHEN LastKind = 'Stability' THEN CAST(JSON_VALUE(LastStats, '$.success_rate') AS Float) ELSE NULL END AS SuccessRate,
    CASE WHEN LastKind = 'Stability' THEN CAST(JSON_VALUE(LastStats, '$.avg_threads_per_iteration') AS Int32) ELSE NULL END AS AvgThreadsPerIteration,
    CASE WHEN LastKind = 'Stability' THEN CAST(JSON_VALUE(LastStats, '$.total_threads') AS Int32) ELSE NULL END AS TotalThreads,
    CASE WHEN LastKind = 'Stability' THEN CAST(JSON_VALUE(LastStats, '$.nodes_percentage') AS Int32) ELSE NULL END AS NodesPercentage,
    CASE WHEN LastKind = 'Stability' THEN CAST(JSON_VALUE(LastStats, '$.nodes_with_issues') AS Int32) ELSE NULL END AS NodesWithIssues,
    
    -- Ошибки и диагностика
    CASE WHEN LastKind = 'Stability' THEN JSON_QUERY(LastStats, '$.node_error_messages') ELSE NULL END AS NodeErrorMessages,
    CASE WHEN LastKind = 'Stability' THEN JSON_QUERY(LastStats, '$.workload_error_messages') ELSE NULL END AS WorkloadErrorMessages,
    CASE WHEN LastKind = 'Stability' THEN JSON_QUERY(LastStats, '$.workload_warning_messages') ELSE NULL END AS WorkloadWarningMessages,
    
    -- Настройки теста
    CASE WHEN LastKind = 'Stability' THEN (CASE WHEN JSON_VALUE(LastStats, '$.use_iterations') = 'true' THEN 1U ELSE 0U END) ELSE NULL END AS UseIterations,
    CASE WHEN LastKind = 'Stability' THEN (CASE WHEN JSON_VALUE(LastStats, '$.nemesis_enabled') = 'true' OR CAST(JSON_VALUE(LastStats, '$.nemesis_enabled') AS Uint64) = 1UL THEN 1U ELSE 0U END) ELSE NULL END AS NemesisEnabled,
    CASE WHEN LastKind = 'Stability' THEN JSON_VALUE(LastStats, '$.workload_type') ELSE NULL END AS WorkloadType,
    CASE WHEN LastKind = 'Stability' THEN JSON_VALUE(LastStats, '$.path_template') ELSE NULL END AS PathTemplate,
    
    -- Статус ошибок
    CASE WHEN LastKind = 'Stability' THEN (CASE WHEN JSON_VALUE(LastStats, '$.node_errors') = 'true' THEN 1U ELSE 0U END) ELSE NULL END AS NodeErrors,
    CASE WHEN LastKind = 'Stability' THEN (CASE WHEN JSON_VALUE(LastStats, '$.workload_errors') = 'true' THEN 1U ELSE 0U END) ELSE NULL END AS WorkloadErrors,
    CASE WHEN LastKind = 'Stability' THEN (CASE WHEN JSON_VALUE(LastStats, '$.workload_warnings') = 'true' THEN 1U ELSE 0U END) ELSE NULL END AS WorkloadWarnings,
    
    -- Дополнительные поля
    CASE WHEN LastKind = 'Stability' THEN JSON_VALUE(LastStats, '$.table_type') ELSE NULL END AS TableType,
    CASE WHEN LastKind = 'Stability' THEN CAST(JSON_VALUE(LastStats, '$.test_timestamp') AS Uint64) ELSE NULL END AS TestTimestamp,
    
    -- Информация о кластере
    JSON_VALUE(LastInfo, '$.cluster.version') AS ClusterVersion,
    CASE
        WHEN JSON_VALUE(LastInfo, '$.cluster.version') IS NOT NULL THEN
            'https://github.com/ydb-platform/ydb/commit/' || String::SplitToList(JSON_VALUE(LastInfo, '$.cluster.version'), '.')[1]
        ELSE NULL
    END AS ClusterVersionLink,
    JSON_VALUE(LastInfo, '$.cluster.endpoint') AS ClusterEndpoint,
    JSON_VALUE(LastInfo, '$.cluster.database') AS ClusterDatabase,
    CASE
        WHEN JSON_VALUE(LastInfo, '$.cluster.endpoint') IS NOT NULL THEN
            String::SplitToList(String::SplitToList(JSON_VALUE(LastInfo, '$.cluster.endpoint'), '//')[1], ':')[0] || ':8765/'
        ELSE NULL
    END AS ClusterMonitoring,
    CAST(JSON_VALUE(LastInfo, '$.cluster.nodes_count') AS Int32) AS NodesCount,
    JSON_QUERY(LastInfo, '$.cluster.nodes_info') AS NodesInfo,
    JSON_VALUE(LastInfo, '$.ci_version') AS CiVersion,
    JSON_VALUE(LastInfo, '$.test_tools_version') AS TestToolsVersion,
    JSON_VALUE(LastInfo, '$.report_url') AS ReportUrl,
    JSON_VALUE(LastInfo, '$.ci_launch_id') AS CiLaunchId,
    JSON_VALUE(LastInfo, '$.ci_launch_url') AS CiLaunchUrl,
    JSON_VALUE(LastInfo, '$.ci_launch_start_time') AS CiLaunchStartTime,
    JSON_VALUE(LastInfo, '$.ci_job_title') AS CiJobTitle,
    JSON_VALUE(LastInfo, '$.ci_cluster_name') AS CiClusterName,
    JSON_VALUE(LastInfo, '$.ci_nemesis') AS CiNemesis,
    JSON_VALUE(LastInfo, '$.ci_build_type') AS CiBuildType,
    JSON_VALUE(LastInfo, '$.ci_sanitizer') AS CiSanitizer,
    
    -- Порядок выполнения
    ROW_NUMBER() OVER (PARTITION BY RunId ORDER BY LastTimestamp, Suite, Test) AS OrderInRun
    
FROM $latest_status;

-- Вычисляем дополнительные метрики
$final_metrics = SELECT
    Db,
    Suite,
    Test,
    RunId,
    LastKind,
    LastTimestamp,
    LastSuccess,
    LastStats,
    LastInfo,
    OverallStatus,
    TotalRuns,
    SuccessfulRuns,
    FailedRuns,
    TotalIterations,
    SuccessfulIterations,
    FailedIterations,
    PlannedDuration,
    ActualDuration,
    TotalExecutionTime,
    SuccessRate,
    AvgThreadsPerIteration,
    TotalThreads,
    NodesPercentage,
    NodesWithIssues,
    NodeErrorMessages,
    WorkloadErrorMessages,
    WorkloadWarningMessages,
    UseIterations,
    NemesisEnabled,
    WorkloadType,
    PathTemplate,
    NodeErrors,
    WorkloadErrors,
    WorkloadWarnings,
    TableType,
    TestTimestamp,
    ClusterVersion,
    ClusterVersionLink,
    ClusterEndpoint,
    ClusterDatabase,
    ClusterMonitoring,
    NodesCount,
    NodesInfo,
    CiVersion,
    TestToolsVersion,
    ReportUrl,
    CiLaunchId,
    CiLaunchUrl,
    CiLaunchStartTime,
    CiJobTitle,
    CiClusterName,
    CiNemesis,
    CiBuildType,
    CiSanitizer,
    OrderInRun,
    
    -- Вычисляем FacedNodeErrors
    CASE
        WHEN NodeErrorMessages IS NULL OR CAST(NodeErrorMessages AS String) = '[]' OR CAST(NodeErrorMessages AS String) = '' THEN NULL
        ELSE
            ListConcat(
                ListUniq(
                    ListNotNull(AsList(
                        CASE WHEN CAST(NodeErrorMessages AS String) LIKE '%coredump%' THEN 'Coredump' ELSE NULL END,
                        CASE WHEN CAST(NodeErrorMessages AS String) LIKE '%OOM%' OR CAST(NodeErrorMessages AS String) LIKE '%experienced OOM%' THEN 'OOM' ELSE NULL END,
                        CASE WHEN CAST(NodeErrorMessages AS String) LIKE '%VERIFY%' OR CAST(NodeErrorMessages AS String) LIKE '%verify%' THEN 'Verify' ELSE NULL END,
                        CASE WHEN CAST(NodeErrorMessages AS String) LIKE '%SAN%' OR CAST(NodeErrorMessages AS String) LIKE '%sanitizer%' THEN 'SAN' ELSE NULL END
                    ))
                ),
                ', '
            )
    END AS FacedNodeErrors,
    
    -- Вычисляем дополнительные метрики
    CASE 
        WHEN TotalRuns > 0 THEN CAST(FailedRuns AS Float) / CAST(TotalRuns AS Float)
        ELSE 0.0
    END AS FailureRate,
    
    CASE 
        WHEN TotalIterations > 0 THEN CAST(FailedIterations AS Float) / CAST(TotalIterations AS Float)
        ELSE 0.0
    END AS IterationFailureRate,
    
    CASE 
        WHEN PlannedDuration > 0 THEN ActualDuration / PlannedDuration
        ELSE NULL
    END AS DurationRatio
    
FROM $test_status;

SELECT
    Db,
    Suite,
    Test,
    CAST(CAST(RunId AS Uint64)/1000UL AS Timestamp) AS RunTs,
    LastTimestamp AS Timestamp,
    LastKind,
    LastSuccess AS Success,
    OverallStatus,
    
    -- Основные метрики
    TotalRuns,
    SuccessfulRuns,
    FailedRuns,
    TotalIterations,
    SuccessfulIterations,
    FailedIterations,
    
    -- Временные метрики
    PlannedDuration,
    ActualDuration,
    TotalExecutionTime,
    
    -- Производительность
    SuccessRate,
    AvgThreadsPerIteration,
    TotalThreads,
    NodesPercentage,
    NodesWithIssues,
    NodeErrorMessages,
    WorkloadErrorMessages,
    WorkloadWarningMessages,
    
    -- Настройки
    UseIterations,
    NemesisEnabled,
    WorkloadType,
    PathTemplate,
    
    -- Статусы ошибок
    NodeErrors,
    WorkloadErrors,
    WorkloadWarnings,
    
    -- Дополнительные поля
    TableType,
    TestTimestamp,
    
    -- Информация об ошибках ClusterCheck
    CASE WHEN LastKind = 'ClusterCheck' THEN JSON_VALUE(LastStats, '$.issue_type') ELSE NULL END AS ClusterIssueType,
    CASE WHEN LastKind = 'ClusterCheck' THEN JSON_VALUE(LastStats, '$.issue_description') ELSE NULL END AS ClusterIssueDescription,
    
    -- Информация о кластере
    ClusterVersion,
    ClusterVersionLink,
    ClusterEndpoint,
    ClusterDatabase,
    ClusterMonitoring,
    NodesCount,
    NodesInfo,
    CiVersion,
    TestToolsVersion,
    ReportUrl,
    CiLaunchId,
    CiLaunchUrl,
    CiLaunchStartTime,
    CiJobTitle,
    CiClusterName,
    CiNemesis,
    CiBuildType,
    CiSanitizer,
    OrderInRun,
    
    -- Извлекаем ветки из версий
    COALESCE(SubString(CAST(ClusterVersion AS String), 0U, FIND(CAST(ClusterVersion AS String), '.')), 'unknown') AS Branch,
    COALESCE(SubString(CAST(CiVersion AS String), 0U, FIND(CAST(CiVersion AS String), '.')), 'unknown') AS CiBranch,
    COALESCE(SubString(CAST(TestToolsVersion AS String), 0U, FIND(CAST(TestToolsVersion AS String), '.')), 'unknown') AS TestToolsBranch,
    
    -- Объединяем BUILD_TYPE и SANITIZER с преобразованием названий
    CASE
        WHEN CiBuildType IS NOT NULL AND CiSanitizer IS NOT NULL AND CiSanitizer != '' THEN
            CiBuildType || '-' || 
            CASE 
                WHEN CiSanitizer = 'address' THEN 'asan'
                WHEN CiSanitizer = 'thread' THEN 'tsan'
                WHEN CiSanitizer = 'memory' THEN 'msan'
                ELSE CiSanitizer
            END
        WHEN CiBuildType IS NOT NULL THEN
            CiBuildType
        ELSE 'unknown'
    END AS BUILD_TYPE_AND_SAN,
    
    -- Вычисляемые метрики
    FailureRate,
    IterationFailureRate,
    DurationRatio,
    FacedNodeErrors,
    
    -- Расширенный статус с дополнительной информацией
    CASE
        -- Если есть FacedNodeErrors, добавляем их к OverallStatus
        WHEN FacedNodeErrors IS NOT NULL AND FacedNodeErrors != '' THEN 
            OverallStatus || ':' || FacedNodeErrors
        
        -- Если infrastructure_error, добавляем информацию об ошибке кластера
        WHEN OverallStatus = 'infrastructure_error' AND LastKind = 'ClusterCheck' THEN
            'infrastructure_error:' || COALESCE(JSON_VALUE(LastStats, '$.issue_type'), 'unknown_issue')
        
        -- Если нет дополнительной информации, используем обычный OverallStatus
        ELSE OverallStatus
    END AS OverallStatusExtended

FROM $final_metrics
ORDER BY LastTimestamp DESC;
