-- Stability Aggregate Data Mart
-- Разворачивание данных для записей с aggregation_level = "aggregate"

$run_id_limit = CAST(CurrentUtcTimestamp() AS Uint64) - 30UL * 86400UL * 1000000UL;

$aggregate_data = SELECT
    Db,
    Suite,
    Test,
    RunId,
    Timestamp,
    Success,
    
    -- Извлекаем основные агрегированные метрики из Stats JSON
    CAST(JSON_VALUE(Stats, '$.total_runs') AS Int32) AS TotalRuns,
    CAST(JSON_VALUE(Stats, '$.successful_runs') AS Int32) AS SuccessfulRuns,
    CAST(JSON_VALUE(Stats, '$.failed_runs') AS Int32) AS FailedRuns,
    CAST(JSON_VALUE(Stats, '$.total_iterations') AS Int32) AS TotalIterations,
    CAST(JSON_VALUE(Stats, '$.successful_iterations') AS Int32) AS SuccessfulIterations,
    CAST(JSON_VALUE(Stats, '$.failed_iterations') AS Int32) AS FailedIterations,
    
    -- Временные метрики
    CAST(JSON_VALUE(Stats, '$.planned_duration') AS Float) AS PlannedDuration,
    CAST(JSON_VALUE(Stats, '$.actual_duration') AS Float) AS ActualDuration,
    CAST(JSON_VALUE(Stats, '$.total_execution_time') AS Float) AS TotalExecutionTime,
    
    -- Метрики производительности
    CAST(JSON_VALUE(Stats, '$.success_rate') AS Float) AS SuccessRate,
    CAST(JSON_VALUE(Stats, '$.avg_threads_per_iteration') AS Int32) AS AvgThreadsPerIteration,
    CAST(JSON_VALUE(Stats, '$.total_threads') AS Int32) AS TotalThreads,
    CAST(JSON_VALUE(Stats, '$.nodes_percentage') AS Int32) AS NodesPercentage,
    CAST(JSON_VALUE(Stats, '$.nodes_with_issues') AS Int32) AS NodesWithIssues,
    
    -- Ошибки и диагностика
    JSON_VALUE(Stats, '$.node_error_messages') AS NodeErrorMessages, -- JSON массив с подробностями ошибок нод
    JSON_VALUE(Stats, '$.workload_error_messages') AS WorkloadErrorMessages, -- JSON массив с ошибками workload
    
    -- Настройки теста
    CAST(JSON_VALUE(Stats, '$.use_iterations') AS Uint8) AS UseIterations,
    CAST(JSON_VALUE(Stats, '$.nemesis') AS Uint8) AS Nemesis,
    CAST(JSON_VALUE(Stats, '$.nemesis_enabled') AS Uint8) AS NemesisEnabled,
    JSON_VALUE(Stats, '$.workload_type') AS WorkloadType,
    JSON_VALUE(Stats, '$.path_template') AS PathTemplate,
    
    -- Статус ошибок и предупреждений
    CAST(JSON_VALUE(Stats, '$.with_errors') AS Uint8) AS WithErrors,
    CAST(JSON_VALUE(Stats, '$.with_warnings') AS Uint8) AS WithWarnings,
    CAST(JSON_VALUE(Stats, '$.with_warrnings') AS Uint8) AS WithWarrnings, -- опечатка в исходных данных
    CAST(JSON_VALUE(Stats, '$.workload_errors') AS Uint8) AS WorkloadErrors,
    CAST(JSON_VALUE(Stats, '$.workload_warnings') AS Uint8) AS WorkloadWarnings,
    JSON_VALUE(Stats, '$.errors') AS ErrorsJson,
    
    -- Дополнительные поля
    JSON_VALUE(Stats, '$.table_type') AS TableType, -- для SimpleQueue workload
    
    -- Временная метка теста
    CAST(JSON_VALUE(Stats, '$.test_timestamp') AS Uint64) AS TestTimestamp,
    CAST(JSON_VALUE(Stats, '$.run_id') AS Uint64) AS StatsRunId,
    
    -- Извлекаем информацию о кластере из Info JSON
    JSON_VALUE(Info, '$.cluster.version') AS ClusterVersion,
    JSON_VALUE(Info, '$.cluster.endpoint') AS ClusterEndpoint,
    JSON_VALUE(Info, '$.cluster.database') AS ClusterDatabase,
    CAST(JSON_VALUE(Info, '$.cluster.nodes_count') AS Int32) AS NodesCount,
    JSON_VALUE(Info, '$.cluster.nodes_info') AS NodesInfo, -- JSON массив с информацией о нодах
    JSON_VALUE(Info, '$.ci_version') AS CiVersion,
    JSON_VALUE(Info, '$.test_tools_version') AS TestToolsVersion,
    JSON_VALUE(Info, '$.report_url') AS ReportUrl,
    JSON_VALUE(Info, '$.ci_launch_id') AS CiLaunchId,
    
FROM `nemesis/tests_results`
WHERE 
    CAST(RunId AS Uint64) / 1000 > $run_id_limit
    AND Kind = 'Stability'
    AND JSON_VALUE(Stats, '$.aggregation_level') = 'aggregate';

SELECT
    Db,
    Suite,
    Test,
    CAST(CAST(RunId AS Uint64)/1000 AS Timestamp) AS RunTs,
    Timestamp,
    Success,
    
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
    
    -- Настройки
    UseIterations,
    Nemesis,
    NemesisEnabled,
    WorkloadType,
    PathTemplate,
    
    -- Статусы
    WithErrors,
    WithWarnings,
    WithWarrnings,
    WorkloadErrors,
    WorkloadWarnings,
    ErrorsJson,
    
    -- Дополнительные поля
    TableType,
    
    -- Временные метки
    TestTimestamp,
    StatsRunId,
    
    -- Информация о кластере
    ClusterVersion,
    ClusterEndpoint,
    ClusterDatabase,
    NodesCount,
    NodesInfo,
    CiVersion,
    TestToolsVersion,
    ReportUrl,
    CiLaunchId,
    
    -- Извлекаем ветки из версий
    COALESCE(SubString(CAST(ClusterVersion AS String), 0U, FIND(CAST(ClusterVersion AS String), '.')), 'unknown') AS Branch,
    COALESCE(SubString(CAST(CiVersion AS String), 0U, FIND(CAST(CiVersion AS String), '.')), 'unknown') AS CiBranch,
    COALESCE(SubString(CAST(TestToolsVersion AS String), 0U, FIND(CAST(TestToolsVersion AS String), '.')), 'unknown') AS TestToolsBranch,
    
    -- Вычисляемые метрики
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
    END AS DurationRatio,
    
    -- Общий статус выполнения
    CASE
        WHEN Success = 1U AND WithErrors = 0U THEN 'success'
        WHEN Success = 1U AND WithErrors = 1U THEN 'success_with_errors'
        WHEN Success = 0U AND WithErrors = 1U THEN 'failure'
        WHEN Success = 0U THEN 'failure'
        ELSE 'unknown'
    END AS OverallStatus

FROM $aggregate_data
ORDER BY RunTs DESC;
