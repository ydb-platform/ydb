-- For Team view by day (muted at start/end, diff, chart OK vs >30) use pre-aggregated table
-- and query: monitoring_queries/muted_tests_daily_by_team_for_bi.sql (source: muted_tests_daily_by_team).
select tm.state_filtered as state_filtered, 
    tm.test_name as test_name, 
    tm.suite_folder as suite_folder, 
    tm.full_name as full_name, 
    tm.date_window as date_window, 
    tm.build_type as build_type, 
    tm.branch as branch, 
    tm.days_ago_window as days_ago_window, 
    tm.pass_count as pass_count, 
    tm.mute_count as mute_count, 
    tm.fail_count as fail_count, 
    tm.skip_count as skip_count, 
    tm.owner as owner, 
    tm.is_muted as is_muted, 
    tm.is_test_chunk as is_test_chunk, 
    tm.state as state, 
    tm.previous_state as previous_state, 
    tm.state_change_date as state_change_date, 
    tm.days_in_state as days_in_state, 
    tm.previous_mute_state as previous_mute_state, 
    tm.mute_state_change_date as mute_state_change_date, 
    tm.days_in_mute_state as days_in_mute_state, 
    tm.previous_state_filtered as previous_state_filtered, 
    tm.state_change_date_filtered as state_change_date_filtered, 
    tm.days_in_state_filtered as days_in_state_filtered,
    CASE 
        WHEN (tm.state = 'Skipped' AND tm.days_in_state > 14) THEN 'Skipped'
        WHEN (tm.state = 'Skipped' AND tm.days_in_state <= 14) THEN 'Skipped_new'
        WHEN tm.days_in_mute_state >= 30 THEN 'MUTED: delete candidate'
        ELSE 'MUTED: in sla'
    END as resolution,
    COALESCE(tm.effective_owner_team, String::ReplaceAll(tm.owner, 'TEAM:@ydb-platform/', '')) AS owner_team,
    CAST(
        CASE 
            WHEN tm.is_muted = 1 OR (tm.state = 'Skipped' AND tm.days_in_state > 14) THEN TRUE
            ELSE FALSE
        END AS Uint8
    ) as is_muted_or_skipped

 FROM `test_results/analytics/tests_monitor` as tm
 where tm.is_test_chunk = 0
 order by date_window desc
