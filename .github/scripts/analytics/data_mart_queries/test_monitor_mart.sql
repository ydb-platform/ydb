SELECT 
    state_filtered, 
    test_name, 
    suite_folder, 
    full_name, 
    date_window, 
    build_type, 
    branch, 
    days_ago_window, 
    pass_count, 
    mute_count, 
    fail_count, 
    skip_count, 
    owner, 
    is_muted, 
    is_test_chunk, 
    state, 
    previous_state, 
    state_change_date, 
    days_in_state, 
    previous_mute_state, 
    mute_state_change_date, 
    days_in_mute_state, 
    previous_state_filtered, 
    state_change_date_filtered, 
    days_in_state_filtered,
    String::ReplaceAll(owner, 'TEAM:@ydb-platform/', '') as owner_team
FROM `test_results/analytics/tests_monitor`
WHERE date_window >= CurrentUtcDate() - 1 * Interval("P1D") -- for init table better take 30* Interval("P1D")
and ( branch = 'main' or branch like 'stable-%')

