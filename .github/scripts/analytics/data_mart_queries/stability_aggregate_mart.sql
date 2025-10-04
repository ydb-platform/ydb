-- Stability Aggregate Data Mart
-- Новая архитектура: TestInit -> ClusterCheck -> Stability

$run_id_limit = CAST(CurrentUtcTimestamp() AS Uint64) - 30UL * 86400UL * 1000000UL;

-- Находим записи инициализации тестов (TestInit) - основа для JOIN
$test_inits = SELECT
    Db,
    Suite,
    Test,
    RunId,
    Timestamp AS InitTimestamp,
    Stats AS InitStats,
    Info AS InitInfo
FROM `nemesis/tests_results`
WHERE 
    CAST(RunId AS Uint64) / 1000UL > $run_id_limit
    AND Kind = 'TestInit';

-- Находим последний ClusterCheck для каждого теста
$cluster_checks = SELECT
    Db,
    Suite,
    Test,
    RunId,
    Timestamp AS CheckTimestamp,
    Success AS CheckSuccess,
    Stats AS CheckStats,
    Info AS CheckInfo,
    JSON_VALUE(Stats, '$.verification_phase') AS VerificationPhase,
    JSON_VALUE(Stats, '$.check_type') AS CheckType,
    ROW_NUMBER() OVER (PARTITION BY Db, Suite, Test, RunId ORDER BY Timestamp DESC) AS rn
FROM `nemesis/tests_results`
WHERE 
    CAST(RunId AS Uint64) / 1000UL > $run_id_limit
    AND Kind = 'ClusterCheck';

-- Берем только последний ClusterCheck для каждого теста с JSON данными
$latest_cluster_checks = SELECT
    Db,
    Suite,
    Test,
    RunId,
    CheckTimestamp,
    CheckSuccess,
    CheckStats,
    VerificationPhase,
    CheckType
FROM $cluster_checks
WHERE rn = 1;

-- Находим Suite со Stability записями
$stability_results = SELECT
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

-- Объединяем все данные: TestInit (основа) + последний ClusterCheck + Stability
$all_tests = SELECT
    ti.Db AS Db,
    ti.Suite AS Suite,
    ti.Test AS Test,
    ti.RunId AS RunId,
    ti.InitTimestamp AS InitTimestamp,
    s.Timestamp AS StabilityTimestamp,
    s.Success AS Success,
    
    -- Информация о последней проверке кластера
    cc.CheckTimestamp AS FirstClusterCheckTimestamp,
    cc.CheckTimestamp AS LastClusterCheckTimestamp,
    
    -- Статус кластерной проверки
    CAST(cc.CheckSuccess AS Uint32) AS OverallClusterCheckSuccess,
    
    -- Информация о проблемах кластера из последней проверки
    JSON_VALUE(cc.CheckStats, '$.issue_type') AS ClusterIssueTypes,
    JSON_VALUE(cc.CheckStats, '$.issue_description') AS ClusterIssueDescriptions,
    cc.VerificationPhase AS VerificationPhases,
    cc.CheckType AS CheckTypes,
    
    -- Извлекаем основные метрики из Stats JSON (NULL для кластерных проблем)
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
    CASE WHEN JSON_VALUE(s.Stats, '$.nemesis') = 'true' OR CAST(JSON_VALUE(s.Stats, '$.nemesis') AS Uint64) = 1UL THEN 1U ELSE 0U END AS Nemesis,
    CASE WHEN JSON_VALUE(s.Stats, '$.nemesis_enabled') = 'true' OR CAST(JSON_VALUE(s.Stats, '$.nemesis_enabled') AS Uint64) = 1UL THEN 1U ELSE 0U END AS NemesisEnabled,
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
    CAST(ti.RunId AS Uint64) AS StatsRunId,
    
    -- Извлекаем информацию о кластере из Info JSON
    JSON_VALUE(COALESCE(s.Info, ti.InitInfo), '$.cluster.version') AS ClusterVersion,
    CASE
        WHEN JSON_VALUE(COALESCE(s.Info, ti.InitInfo), '$.cluster.version') IS NOT NULL THEN
            'https://github.com/ydb-platform/ydb/commit/' || String::SplitToList(JSON_VALUE(COALESCE(s.Info, ti.InitInfo), '$.cluster.version'), '.')[1]
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
    JSON_VALUE(COALESCE(s.Info, ti.InitInfo), '$.ci_nemesis') AS CiNemesis,
    JSON_VALUE(COALESCE(s.Info, ti.InitInfo), '$.ci_build_type') AS CiBuildType,
    JSON_VALUE(COALESCE(s.Info, ti.InitInfo), '$.ci_sanitizer') AS CiSanitizer,
    
    -- Порядок выполнения
    ROW_NUMBER() OVER (PARTITION BY ti.RunId ORDER BY ti.InitTimestamp, ti.Suite, ti.Test) AS OrderInRun
    
FROM $test_inits AS ti
LEFT JOIN $latest_cluster_checks AS cc
    ON ti.Db = cc.Db 
    AND ti.RunId = cc.RunId
    AND ti.Suite = cc.Suite
    AND ti.Test = cc.Test
LEFT JOIN $stability_results AS s
    ON ti.Db = s.Db 
    AND ti.RunId = s.RunId
    AND ti.Suite = s.Suite
    AND ti.Test = s.Test;

-- Вычисляем статус
$test_status = SELECT
    -- Основные поля
    Db,
    Suite,
    Test,
    RunId,
    InitTimestamp,
    StabilityTimestamp,
    Success,
    OrderInRun,
    
    -- Статистика тестов
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
    
    -- Метрики производительности
    SuccessRate,
    AvgThreadsPerIteration,
    TotalThreads,
    NodesPercentage,
    NodesWithIssues,
    
    -- Ошибки и диагностика
    NodeErrorMessages,
    WorkloadErrorMessages,
    WorkloadWarningMessages,
    
    -- Настройки теста
    UseIterations,
    Nemesis,
    NemesisEnabled,
    WorkloadType,
    PathTemplate,
    
    -- Статус ошибок
    NodeErrors,
    WorkloadErrors,
    WorkloadWarnings,
    
    -- Дополнительные поля
    Stats,
    TableType,
    TestTimestamp,
    StatsRunId,
    
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
    
    -- ClusterCheck поля (агрегированные)
    OverallClusterCheckSuccess,
    FirstClusterCheckTimestamp,
    LastClusterCheckTimestamp,
    ClusterIssueTypes,
    ClusterIssueDescriptions,
    VerificationPhases,
    CheckTypes,
    
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
    
    -- Вычисляем OverallStatus
    CASE
        -- Приоритет 1: Проблемы с инфраструктурой (ClusterCheck failed)
        WHEN OverallClusterCheckSuccess = 0U THEN 'infrastructure_error'
        
        -- Приоритет 2: Кластер OK, но нет Stability записи (тест завис/не завершился)
        WHEN OverallClusterCheckSuccess = 1U AND Success IS NULL THEN 
            CASE 
                -- Если прошло больше чем planned_duration * 3 от времени старта первой проверки кластера, считаем timeout
                WHEN CAST(CurrentUtcTimestamp() AS Uint64) - CAST(FirstClusterCheckTimestamp AS Uint64) > CAST(COALESCE(PlannedDuration, 1800) * 3 * 1000000 AS Uint64) THEN 'timeout'
                ELSE 'in_progress'
            END
        
        -- Приоритет 3: Обычные workload тесты (если кластер OK и есть Stability запись)
        WHEN Success = 1U AND (NodeErrors IS NULL OR NodeErrors = 0U) AND (WorkloadErrors IS NULL OR WorkloadErrors = 0U) THEN 'success'
        WHEN Success = 1U AND (NodeErrors = 1U OR WorkloadErrors = 1U) THEN 'success_with_errors'
        WHEN Success = 0U AND NodeErrors = 1U THEN 'node_failure'
        WHEN Success = 0U AND WorkloadErrors = 1U THEN 'success' -- 'workload_failure'
        WHEN Success = 0U THEN 'failure'
        ELSE 'unknown'
    END AS OverallStatus
FROM $all_tests;

SELECT
    agg.Db,
    agg.Suite,
    agg.Test,
    CAST(CAST(agg.RunId AS Uint64)/1000UL AS Timestamp) AS RunTs,
    agg.StabilityTimestamp AS Timestamp,
    
    -- Четкие поля времени старта и окончания
    agg.InitTimestamp AS TestInitTime,                    -- Время инициализации теста
    agg.InitTimestamp AS InitTimestamp,                   -- Время инициализации теста (для совместимости)
    agg.FirstClusterCheckTimestamp AS FirstCheckTime,    -- Время первой проверки кластера
    agg.LastClusterCheckTimestamp AS LastCheckTime,      -- Время последней проверки кластера
    agg.StabilityTimestamp AS EndTime,                   -- Время выгрузки результатов (окончание всего теста)
    
    -- Времена workload из Stats
    CAST(JSON_VALUE(agg.Stats, '$.workload_start_time') AS Timestamp) AS WorkloadStartTime,
    CAST(JSON_VALUE(agg.Stats, '$.workload_end_time') AS Timestamp) AS WorkloadEndTime,
    CAST(JSON_VALUE(agg.Stats, '$.workload_duration') AS Float) AS WorkloadDurationSeconds,
    
    -- Общая длительность теста (от инициализации до выгрузки)
    CASE 
        WHEN agg.StabilityTimestamp IS NOT NULL AND agg.InitTimestamp IS NOT NULL THEN
            CAST(agg.StabilityTimestamp - agg.InitTimestamp AS Float) / 1000000.0  -- микросекунды в секунды
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
    
    -- Информация о кластерных проверках
    agg.OverallClusterCheckSuccess,
    agg.ClusterIssueTypes,
    agg.ClusterIssueDescriptions,
    agg.VerificationPhases,
    agg.CheckTypes,
    
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
    
    -- Типы найденных ошибок нод (уже вычислено в $test_status)
    agg.FacedNodeErrors,
    
    -- Общий статус выполнения (уже вычислен в $test_status)
    agg.OverallStatus,
    
    -- Расширенный статус с дополнительной информацией
    CASE
        -- Если есть FacedNodeErrors, добавляем их к OverallStatus
        WHEN agg.FacedNodeErrors IS NOT NULL AND agg.FacedNodeErrors != '' THEN 
            CASE
                WHEN agg.OverallClusterCheckSuccess = 0U THEN 'infrastructure_error:' || agg.ClusterIssueTypes
                ELSE agg.OverallStatus || ':' || agg.FacedNodeErrors
            END
        
        -- Если нет FacedNodeErrors, но есть ClusterIssueTypes, добавляем их
        WHEN agg.ClusterIssueTypes IS NOT NULL AND agg.ClusterIssueTypes != '' THEN 
            'infrastructure_error:' || agg.ClusterIssueTypes
        
        -- Если нет дополнительной информации, используем обычный OverallStatus
        ELSE agg.OverallStatus
    END AS OverallStatusExtended

FROM $test_status AS agg
ORDER BY InitTimestamp DESC;
