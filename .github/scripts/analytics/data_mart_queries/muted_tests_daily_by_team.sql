-- Pre-aggregation: muted tests by team and by day, per build_type.
-- Filters: is_test_chunk = 0, is_muted = 1 (as required for BI).
-- One row per (date_window, owner_team, branch, build_type). In BI: filter by build_type for per-config metrics, or sum for total.
SELECT
    tm.date_window AS date_window,
    String::ReplaceAll(tm.owner, 'TEAM:@ydb-platform/', '') AS owner_team,
    tm.branch AS branch,
    tm.build_type AS build_type,
    COUNT(DISTINCT tm.full_name) AS muted_count,
    COUNT(DISTINCT CASE WHEN tm.days_in_mute_state >= 30 THEN tm.full_name ELSE NULL END) AS muted_count_more_30_days
FROM `test_results/analytics/tests_monitor` AS tm
WHERE 
  tm.date_window >= CurrentUtcDate() - 365 * Interval("P1D")
  and tm.build_type = 'relwithdebinfo' 
  and tm.is_test_chunk = 0
  AND tm.is_muted = 1
  
GROUP BY
    tm.date_window,
    tm.owner,
    tm.branch,
    tm.build_type;
