-- Stability Per-Run Data Mart
-- Разворачивание данных для записей с aggregation_level = "per_run"

$run_id_limit = CAST(CurrentUtcTimestamp() AS Uint64) - 30UL * 86400UL * 1000000UL;

$per_run_data = SELECT
    Db,
    Suite,
    Test,
    RunId,
    Timestamp,
    Success,
    MeanDuration / 1000. AS Duration, -- конвертируем в секунды
    
    -- Извлекаем основные поля из Stats JSON
    CAST(JSON_VALUE(Stats, '$.iteration') AS Int32) AS Iteration,
    CAST(JSON_VALUE(Stats, '$.run_index') AS Int32) AS RunIndex,
    JSON_VALUE(Stats, '$.resolution') AS Resolution,
    CASE WHEN JSON_VALUE(Stats, '$.nemesis_enabled') = 'true' THEN 1U ELSE 0U END AS NemesisEnabled,
    JSON_VALUE(Stats, '$.error_message') AS ErrorMessage,
    JSON_VALUE(Stats, '$.warning_message') AS WarningMessage,
    JSON_QUERY(Stats, '$.node_error_messages') AS NodeErrorMessages, -- JSON массив с ошибками нод (если доступен)
    CAST(JSON_VALUE(Stats, '$.run_id') AS Uint64) AS StatsRunId,
    
    -- Поля для кластерных проблем (NULL для обычных записей)
    NULL AS ClusterIssueType,
    NULL AS ClusterIssueDescription,
    NULL AS ClusterIssuePhase,
    0U AS ClusterIssueCritical,
    
    -- Извлекаем информацию о кластере из Info JSON
    JSON_VALUE(Info, '$.cluster.version') AS ClusterVersion,
    -- Создаем ссылку на GitHub commit из версии кластера (main.108fc20 -> https://github.com/ydb-platform/ydb/commit/108fc20)
    CASE
        WHEN JSON_VALUE(Info, '$.cluster.version') IS NOT NULL THEN
            'https://github.com/ydb-platform/ydb/commit/' || String::SplitToList(JSON_VALUE(Info, '$.cluster.version'), '.')[1]
        ELSE NULL
    END AS ClusterVersionLink,
    JSON_VALUE(Info, '$.cluster.endpoint') AS ClusterEndpoint,
    JSON_VALUE(Info, '$.cluster.database') AS ClusterDatabase,
    -- Извлекаем мониторинг кластера из endpoint (@grpc://host:port/ -> host:monitoring_port)
        CASE
            WHEN JSON_VALUE(Info, '$.cluster.endpoint') IS NOT NULL THEN
                String::SplitToList(String::SplitToList(JSON_VALUE(Info, '$.cluster.endpoint'), '//')[1], ':')[0] || ':8765'
            ELSE NULL
        END AS ClusterMonitoring,
    CAST(JSON_VALUE(Info, '$.cluster.nodes_count') AS Int32) AS NodesCount,
    JSON_QUERY(Info, '$.cluster.nodes_info') AS NodesInfo, -- JSON массив с информацией о нодах
    JSON_VALUE(Info, '$.ci_version') AS CiVersion,
    JSON_VALUE(Info, '$.test_tools_version') AS TestToolsVersion,
    JSON_VALUE(Info, '$.report_url') AS ReportUrl,
    JSON_VALUE(Info, '$.ci_launch_id') AS CiLaunchId,
    JSON_VALUE(Info, '$.ci_launch_url') AS CiLaunchUrl,
    JSON_VALUE(Info, '$.ci_launch_start_time') AS CiLaunchStartTime,
    JSON_VALUE(Info, '$.ci_job_title') AS CiJobTitle,
    JSON_VALUE(Info, '$.ci_cluster_name') AS CiClusterName,
    JSON_VALUE(Info, '$.ci_nemesis') AS CiNemesis,
    JSON_VALUE(Info, '$.ci_build_type') AS CiBuildType,
    JSON_VALUE(Info, '$.ci_sanitizer') AS CiSanitizer,
    
    -- Порядок выполнения теста в рамках RunId (на основе Timestamp)
    ROW_NUMBER() OVER (PARTITION BY RunId ORDER BY Timestamp) AS OrderInRun

FROM `nemesis/tests_results`
WHERE 
    CAST(RunId AS Uint64) / 1000 > $run_id_limit
    AND Kind = 'Stability'
    AND JSON_VALUE(Stats, '$.aggregation_level') = 'per_run'

UNION ALL

-- Добавляем записи ClusterCheck как специальные per_run записи
SELECT
    Db,
    Suite,
    Test,
    RunId,
    Timestamp,
    Success,
    0.0 AS Duration, -- Кластерные проблемы не имеют длительности
    
    -- Поля per_run (NULL для кластерных проблем)
    NULL AS Iteration,
    NULL AS RunIndex,
    CASE 
        WHEN JSON_VALUE(Stats, '$.issue_type') = 'cluster_not_alive' THEN 'cluster_not_alive'
        WHEN JSON_VALUE(Stats, '$.issue_type') = 'cluster_no_nodes' THEN 'cluster_no_nodes'
        WHEN JSON_VALUE(Stats, '$.issue_type') = 'cluster_check_exception' THEN 'cluster_check_exception'
        WHEN JSON_VALUE(Stats, '$.issue_type') = 'cluster_unknown_issue' THEN 'cluster_unknown_issue'
        WHEN Success = 0U THEN 'cluster_issue'
        ELSE 'cluster_ok'
    END AS Resolution,
    0U AS NemesisEnabled,
    JSON_VALUE(Stats, '$.issue_description') AS ErrorMessage,
    NULL AS WarningMessage,
    NULL AS NodeErrorMessages,
    CAST(RunId AS Uint64) AS StatsRunId,
    
    -- Поля для кластерных проблем
    JSON_VALUE(Stats, '$.issue_type') AS ClusterIssueType,
    JSON_VALUE(Stats, '$.issue_description') AS ClusterIssueDescription,
    JSON_VALUE(Stats, '$.verification_phase') AS ClusterIssuePhase,
    CASE WHEN JSON_VALUE(Stats, '$.is_critical') = 'true' THEN 1U ELSE 0U END AS ClusterIssueCritical,
    
    -- Извлекаем информацию о кластере из Info JSON (может быть NULL для кластерных проблем)
    JSON_VALUE(Info, '$.cluster.version') AS ClusterVersion,
    CASE
        WHEN JSON_VALUE(Info, '$.cluster.version') IS NOT NULL THEN
            'https://github.com/ydb-platform/ydb/commit/' || String::SplitToList(JSON_VALUE(Info, '$.cluster.version'), '.')[1]
        ELSE NULL
    END AS ClusterVersionLink,
    JSON_VALUE(Info, '$.cluster.endpoint') AS ClusterEndpoint,
    JSON_VALUE(Info, '$.cluster.database') AS ClusterDatabase,
    CASE
        WHEN JSON_VALUE(Info, '$.cluster.endpoint') IS NOT NULL THEN
            String::SplitToList(String::SplitToList(JSON_VALUE(Info, '$.cluster.endpoint'), '//')[1], ':')[0] || ':8765'
        ELSE NULL
    END AS ClusterMonitoring,
    CAST(JSON_VALUE(Info, '$.cluster.nodes_count') AS Int32) AS NodesCount,
    JSON_QUERY(Info, '$.cluster.nodes_info') AS NodesInfo,
    JSON_VALUE(Info, '$.ci_version') AS CiVersion,
    JSON_VALUE(Info, '$.test_tools_version') AS TestToolsVersion,
    JSON_VALUE(Info, '$.report_url') AS ReportUrl,
    JSON_VALUE(Info, '$.ci_launch_id') AS CiLaunchId,
    JSON_VALUE(Info, '$.ci_launch_url') AS CiLaunchUrl,
    JSON_VALUE(Info, '$.ci_launch_start_time') AS CiLaunchStartTime,
    JSON_VALUE(Info, '$.ci_job_title') AS CiJobTitle,
    JSON_VALUE(Info, '$.ci_cluster_name') AS CiClusterName,
    JSON_VALUE(Info, '$.ci_nemesis') AS CiNemesis,
    JSON_VALUE(Info, '$.ci_build_type') AS CiBuildType,
    JSON_VALUE(Info, '$.ci_sanitizer') AS CiSanitizer,
    
    -- Порядок выполнения теста в рамках RunId (на основе Timestamp)
    ROW_NUMBER() OVER (PARTITION BY RunId ORDER BY Timestamp) AS OrderInRun

FROM `nemesis/tests_results`
WHERE 
    CAST(RunId AS Uint64) / 1000 > $run_id_limit
    AND Kind = 'ClusterCheck';

SELECT
    Db,
    Suite,
    Test,
    CAST(CAST(RunId AS Uint64)/1000 AS Timestamp) AS RunTs,
    Timestamp,
    Success,
    Duration,
    Iteration,
    RunIndex,
    Resolution,
    NemesisEnabled,
    ErrorMessage,
    WarningMessage,
    NodeErrorMessages,
    StatsRunId,
    ClusterVersion,
    ClusterVersionLink,
    ClusterEndpoint,
    ClusterDatabase,
    ClusterMonitoring,
    NodesCount,
    NodesInfo,
    CiVersion,
    CiLaunchUrl,
    CiLaunchStartTime,
    CiJobTitle,
    CiClusterName,
    CiNemesis,
    CiBuildType,
    CiSanitizer,
    TestToolsVersion,
    ReportUrl,
    CiLaunchId,
    ClusterIssueType,
    ClusterIssueDescription,
    ClusterIssuePhase,
    ClusterIssueCritical,
    OrderInRun,
    
    -- Извлекаем ветку из версии
    COALESCE(SubString(CAST(ClusterVersion AS String), 0U, FIND(CAST(ClusterVersion AS String), '.')), 'unknown') AS Branch,
    COALESCE(SubString(CAST(CiVersion AS String), 0U, FIND(CAST(CiVersion AS String), '.')), 'unknown') AS CiBranch,
    COALESCE(SubString(CAST(TestToolsVersion AS String), 0U, FIND(CAST(TestToolsVersion AS String), '.')), 'unknown') AS TestToolsBranch,
    
    -- Типы найденных ошибок нод (анализ node_error_messages)
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
    
    -- Статус выполнения с детализацией кластерных проблем
    CASE
        -- Конкретные типы кластерных проблем
        WHEN Resolution = 'cluster_not_alive' THEN 'cluster_not_alive'
        WHEN Resolution = 'cluster_no_nodes' THEN 'cluster_no_nodes'
        WHEN Resolution = 'cluster_check_exception' THEN 'cluster_check_exception'
        WHEN Resolution = 'cluster_unknown_issue' THEN 'cluster_unknown_issue'
        WHEN Resolution = 'cluster_issue' THEN 'cluster_issue'  -- Fallback
        WHEN Resolution = 'cluster_ok' THEN 'cluster_check_passed'
        
        -- Обычные статусы workload
        WHEN Resolution = 'ok' THEN 'success'
        WHEN Resolution = 'error' THEN 'error'
        WHEN Resolution = 'timeout' THEN 'timeout'
        WHEN Resolution = 'warning' THEN 'warning'
        ELSE 'unknown'
    END AS ExecutionStatus

FROM $per_run_data
ORDER BY RunTs DESC, Iteration ASC, RunIndex ASC;
