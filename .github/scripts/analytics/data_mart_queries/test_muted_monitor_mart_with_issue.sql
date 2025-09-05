SELECT 
    tm.state_filtered, 
    tm.test_name, 
    tm.suite_folder, 
    tm.full_name, 
    tm.date_window, 
    tm.build_type, 
    tm.branch, 
    tm.days_ago_window, 
    tm.pass_count, 
    tm.mute_count, 
    tm.fail_count, 
    tm.skip_count, 
    tm.owner, 
    tm.is_muted, 
    tm.is_test_chunk, 
    tm.state, 
    tm.previous_state, 
    tm.state_change_date, 
    tm.days_in_state, 
    tm.previous_mute_state, 
    tm.mute_state_change_date, 
    tm.days_in_mute_state, 
    tm.previous_state_filtered, 
    tm.state_change_date_filtered, 
    tm.days_in_state_filtered,
    CASE 
        WHEN (tm.state = 'Skipped' AND tm.days_in_state > 14) THEN 'Skipped'
        WHEN tm.days_in_mute_state >= 30 THEN 'MUTED: delete candidate'
        ELSE 'MUTED: in sla'
    END as resolution,
    String::ReplaceAll(tm.owner, 'TEAM:@ydb-platform/', '') as owner_team,
    CASE 
        WHEN tm.is_muted = 1 OR (tm.state = 'Skipped' AND tm.days_in_state > 14) THEN TRUE
        ELSE FALSE
    END as is_muted_or_skipped,
    gim.github_issue_url,
    gim.github_issue_title,
    gim.github_issue_number
FROM `test_results/analytics/tests_monitor` AS tm
LEFT JOIN `test_results/analytics/github_issue_mapping` AS gim
    ON tm.full_name = gim.full_name
WHERE tm.date_window >= CurrentUtcDate() - 30 * Interval("P1D")
and ( tm.branch = 'main' or tm.branch like 'stable-%')
and tm.is_test_chunk = 0
and (CASE 
        WHEN tm.is_muted = 1 OR (tm.state = 'Skipped' AND tm.days_in_state > 14) THEN TRUE
        ELSE FALSE
    END ) = TRUE