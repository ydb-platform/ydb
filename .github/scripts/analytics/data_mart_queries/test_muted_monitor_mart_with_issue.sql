PRAGMA AnsiInForEmptyOrNullableItemsCollections;

$default_unmute_days = 7u;
$manual_fast_unmute_days = 1u;
$manual_wait_hours = $manual_fast_unmute_days * 24u;

SELECT 
    tm.state_filtered as state_filtered, 
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
        WHEN tm.days_in_mute_state >= 30 THEN 'MUTED: delete candidate'
        ELSE 'MUTED: in sla'
    END as resolution,
    String::ReplaceAll(tm.owner, 'TEAM:@ydb-platform/', '') as owner_team,
    CAST(
        CASE 
            WHEN tm.is_muted = 1 OR (tm.state = 'Skipped' AND tm.days_in_state > 14) THEN TRUE
            ELSE FALSE
        END AS Uint8
    ) as is_muted_or_skipped,
    gim.github_issue_url as github_issue_url,
    gim.github_issue_number as github_issue_number,
    gim.github_issue_state as github_issue_state,
    gim.github_issue_created_at as github_issue_created_at,
    mru.manual_unmute_status as manual_unmute_status,
    CAST(mru.manual_request_active AS Uint8) as is_manual_unmute_requested,
    mru.manual_requested_at as manual_unmute_requested_at,
    CAST(Coalesce(mru.effective_unmute_window_days, $default_unmute_days) AS Uint32) as effective_unmute_window_days,
    CAST(Coalesce(mru.default_unmute_window_days, $default_unmute_days) AS Uint32) as default_unmute_window_days,
    CAST(Coalesce(mru.manual_fast_unmute_window_days, $manual_fast_unmute_days) AS Uint32) as manual_fast_unmute_window_days,
    CAST(Coalesce(mru.manual_wait_hours, $manual_wait_hours) AS Uint32) as manual_unmute_wait_hours,
    CAST(Coalesce(mru.hours_until_ready, 0u) AS Uint32) as manual_unmute_hours_until_ready
FROM `test_results/analytics/tests_monitor` AS tm
LEFT JOIN `test_results/analytics/github_issue_mapping` AS gim
    ON tm.full_name = gim.full_name
    AND tm.branch = gim.branch
LEFT JOIN (
    SELECT
        full_name,
        branch,
        build_type,
        manual_unmute_status,
        manual_request_active,
        manual_requested_at,
        hours_until_ready,
        effective_unmute_window_days,
        default_unmute_window_days,
        manual_fast_unmute_window_days,
        manual_wait_hours
    FROM (
        SELECT
            m.*,
            ROW_NUMBER() OVER (
                PARTITION BY m.full_name, m.branch, m.build_type
                ORDER BY m.exported_at DESC, m.issue_number DESC
            ) AS rn
        FROM `test_results/analytics/manual_unmute_requests` AS m
    )
    WHERE rn = 1
) AS mru
    ON tm.full_name = mru.full_name
    AND tm.branch = mru.branch
    AND tm.build_type = mru.build_type
WHERE tm.date_window >= CurrentUtcDate() - 2 * Interval("P1D")
    AND (tm.branch = 'main' OR tm.branch LIKE 'stable-%' OR tm.branch LIKE 'stream-nb-25%')
    AND tm.is_test_chunk = 0
