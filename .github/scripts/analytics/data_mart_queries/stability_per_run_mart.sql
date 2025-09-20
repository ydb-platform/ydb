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
    CAST(JSON_VALUE(Stats, '$.run_id') AS Uint64) AS StatsRunId,
    -- NOTE: Информация об ошибках нод доступна только в агрегированных данных
    -- чтобы избежать неточной временной привязки ошибок к конкретным запускам
    
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
    AND JSON_VALUE(Stats, '$.aggregation_level') = 'per_run';

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
    StatsRunId,
    ClusterVersion,
    ClusterEndpoint,
    ClusterDatabase,
    NodesCount,
    NodesInfo,
    CiVersion,
    TestToolsVersion,
    ReportUrl,
    CiLaunchId,
    
    -- Извлекаем ветку из версии
    COALESCE(SubString(CAST(ClusterVersion AS String), 0U, FIND(CAST(ClusterVersion AS String), '.')), 'unknown') AS Branch,
    COALESCE(SubString(CAST(CiVersion AS String), 0U, FIND(CAST(CiVersion AS String), '.')), 'unknown') AS CiBranch,
    COALESCE(SubString(CAST(TestToolsVersion AS String), 0U, FIND(CAST(TestToolsVersion AS String), '.')), 'unknown') AS TestToolsBranch,
    
    -- Статус выполнения
    CASE
        WHEN Resolution = 'ok' THEN 'success'
        WHEN Resolution = 'error' THEN 'error'
        WHEN Resolution = 'timeout' THEN 'timeout'
        WHEN Resolution = 'warning' THEN 'warning'
        ELSE 'unknown'
    END AS ExecutionStatus

FROM $per_run_data
ORDER BY RunTs DESC, Iteration ASC, RunIndex ASC;
