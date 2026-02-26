-- Last update time per step in 3 chains. Version without CTE (inline subqueries). Source: analytics/query_statistics.
-- Chain 1: muted_tests (testowners -> ... -> tests_monitor -> github_issue_mapping -> muted_tests_daily_by_team). Chain 2: area_to_owner. Chain 3: github_issues (github_issues_export -> github_issue_mapping -> github_issues_timeline -> github_issues_bugs_count_by_period).

SELECT
    c.chain_order AS chain_order,
    c.chain_name AS chain_name,
    c.step_name AS step_name,
    c.step_order AS step_order,
    s.last_updated AS last_updated,
    DateTime::ToHours(CurrentUtcTimestamp() - s.last_updated) AS hours_ago,
    s.last_script_name AS last_script_name,
    s.last_workflow AS last_workflow
FROM (
    SELECT 1 AS chain_order, "muted_tests" AS chain_name, "testowners" AS step_name, 1 AS step_order
    UNION ALL SELECT 1 AS chain_order, "muted_tests" AS chain_name, "flaky_tests_window" AS step_name, 2 AS step_order
    UNION ALL SELECT 1 AS chain_order, "muted_tests" AS chain_name, "all_tests_with_owner_and_mute" AS step_name, 3 AS step_order
    UNION ALL SELECT 1 AS chain_order, "muted_tests" AS chain_name, "tests_monitor" AS step_name, 4 AS step_order
    UNION ALL SELECT 1 AS chain_order, "muted_tests" AS chain_name, "github_issue_mapping" AS step_name, 5 AS step_order
    UNION ALL SELECT 1 AS chain_order, "muted_tests" AS chain_name, "muted_tests_daily_by_team" AS step_name, 6 AS step_order
    UNION ALL SELECT 2 AS chain_order, "area_to_owner" AS chain_name, "area_to_owner_mapping" AS step_name, 1 AS step_order
    UNION ALL SELECT 3 AS chain_order, "github_issues" AS chain_name, "github_issues_export" AS step_name, 1 AS step_order
    UNION ALL SELECT 3 AS chain_order, "github_issues" AS chain_name, "github_issue_mapping" AS step_name, 2 AS step_order
    UNION ALL SELECT 3 AS chain_order, "github_issues" AS chain_name, "github_issues_timeline" AS step_name, 3 AS step_order
    UNION ALL SELECT 3 AS chain_order, "github_issues" AS chain_name, "github_issues_bugs_count_by_period" AS step_name, 4 AS step_order

    
) AS c
LEFT JOIN (
    SELECT
        step_name,
        MAX(timestamp) AS last_updated,
        MAX_BY(script_name, timestamp) AS last_script_name,
        MAX_BY(github_workflow_name, timestamp) AS last_workflow
    FROM (
        SELECT
            timestamp,
            script_name,
            github_workflow_name,
            CASE
                WHEN table_path LIKE '%testowners' OR script_name = 'upload_testowners.py' THEN "testowners"
                WHEN table_path LIKE '%flaky_tests_window%' OR script_name = 'flaky_tests_history.py' THEN "flaky_tests_window"
                WHEN table_path LIKE '%all_tests_with_owner_and_mute%' OR script_name = 'get_muted_tests.py' THEN "all_tests_with_owner_and_mute"
                WHEN table_path LIKE '%/tests_monitor' OR script_name = 'tests_monitor.py' OR query_name LIKE 'tests_monitor%' THEN "tests_monitor"
                WHEN query_name = 'test_monitor_aggregated_by_team' OR table_path LIKE '%test_monitor_aggregated_by_team%' THEN "test_monitor_aggregated_by_team"
                WHEN query_name = 'muted_tests_daily_by_team' OR table_path LIKE '%muted_tests_daily_by_team%' THEN "muted_tests_daily_by_team"
                WHEN query_name = 'github_issues_timeline' OR table_path LIKE '%github_issues_timeline%' THEN "github_issues_timeline"
                WHEN table_path LIKE '%github_issue_mapping%' OR script_name = 'github_issue_mapping.py' THEN "github_issue_mapping"
                WHEN table_path LIKE '%github_issues_bugs_count_by_period%' THEN "github_issues_bugs_count_by_period"
                WHEN script_name = 'export_issues_to_ydb.py' OR (table_path LIKE '%/issues' AND table_path NOT LIKE '%github_issue_mapping%' AND table_path NOT LIKE '%github_issues_timeline%') THEN "github_issues_export"
                WHEN table_path LIKE '%area_to_owner_mapping%' OR script_name = 'sync_area_to_owner_mapping.py' THEN "area_to_owner_mapping"
                ELSE NULL
            END AS step_name
        FROM `analytics/query_statistics`
        WHERE operation_type = 'bulk_upsert'
    ) AS stats
    WHERE step_name IS NOT NULL
    GROUP BY step_name
) AS s ON Cast(s.step_name AS Utf8) = Cast(c.step_name AS Utf8)
ORDER BY c.chain_order, c.step_order
