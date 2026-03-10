-- BI: Team view by day. Source: pre-aggregated table test_results/analytics/muted_tests_daily_by_team.
-- Filters already applied in pre-aggregation: is_test_chunk = 0, is_muted = 1.
-- In BI use: Team = owner_team, Date interval = range of date_window.
-- Muted at start = muted_count for first day in interval; Muted at end = muted_count for last day; Diff = end - start.
-- Chart: muted_count_ok (blue) and muted_count_more_30_days (red) by date_window.
SELECT
    area,
    date_window,
    owner_team,
    branch,
    build_type,
    muted_count,
    muted_count_more_30_days,
    muted_count - muted_count_more_30_days AS muted_count_ok
FROM `test_results/analytics/muted_tests_daily_by_team`
ORDER BY date_window DESC;
