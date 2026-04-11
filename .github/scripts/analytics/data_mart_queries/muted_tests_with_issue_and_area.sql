$window_days = 365;
$default_unmute_days = 7u;
$manual_fast_unmute_days = 1u;
$manual_wait_hours = $manual_fast_unmute_days * 24u;

SELECT
    tm.state_filtered AS state_filtered,
    tm.test_name AS test_name,
    tm.suite_folder AS suite_folder,
    tm.full_name AS full_name,
    tm.date_window AS date_window,
    tm.build_type AS build_type,
    tm.branch AS branch,
    tm.days_ago_window AS days_ago_window,
    tm.pass_count AS pass_count,
    tm.mute_count AS mute_count,
    tm.fail_count AS fail_count,
    tm.skip_count AS skip_count,
    tm.owner AS owner,
    tm.is_muted AS is_muted,
    tm.is_test_chunk AS is_test_chunk,
    tm.state AS state,
    tm.previous_state AS previous_state,
    tm.state_change_date AS state_change_date,
    tm.days_in_state AS days_in_state,
    tm.previous_mute_state AS previous_mute_state,
    tm.mute_state_change_date AS mute_state_change_date,
    tm.days_in_mute_state AS days_in_mute_state,
    tm.previous_state_filtered AS previous_state_filtered,
    tm.state_change_date_filtered AS state_change_date_filtered,
    tm.days_in_state_filtered AS days_in_state_filtered,
    tm.owner_team_key AS owner_team,
    Coalesce(om.area, 'area/-') AS area,
    Coalesce(mu.manual_unmute_status, 'none') AS manual_unmute_status,
    mu.resolution_reason AS manual_unmute_reason,
    mu.manual_requested_at AS manual_unmute_requested_at,
    Coalesce(mu.manual_wait_hours, $manual_wait_hours) AS manual_unmute_wait_hours,
    Coalesce(mu.hours_until_ready, 0u) AS manual_unmute_hours_until_ready,
    Coalesce(mu.effective_unmute_window_days, $default_unmute_days) AS effective_unmute_window_days,
    Coalesce(mu.default_unmute_window_days, $default_unmute_days) AS default_unmute_window_days,
    Coalesce(mu.manual_fast_unmute_window_days, $manual_fast_unmute_days) AS manual_fast_unmute_window_days,
    gim.github_issue_url AS github_issue_url,
    gim.github_issue_number AS github_issue_number,
    gim.github_issue_state AS github_issue_state,
    gim.github_issue_created_at AS github_issue_created_at
FROM (
    SELECT
        t.*,
        Unicode::ToLower(Cast(Coalesce(String::ReplaceAll(t.owner, 'TEAM:@ydb-platform/', ''), '') AS Utf8)) AS owner_team_key
    FROM `test_results/analytics/tests_monitor` AS t
    WHERE
      t.date_window >= CurrentUtcDate() - $window_days * Interval("P1D")
      and t.branch = 'main'
      and t.build_type = 'relwithdebinfo'
        and t.is_test_chunk = 0
        AND t.is_muted = 1
        and t.state != 'Skipped'

) AS tm
LEFT JOIN (
    SELECT owner_team AS owner_team, MIN(area) AS area
    FROM `test_results/analytics/area_to_owner_mapping`
    GROUP BY owner_team
) AS om ON tm.owner_team_key = om.owner_team
LEFT JOIN (
    SELECT
        full_name,
        branch,
        build_type,
        manual_unmute_status,
        resolution_reason,
        manual_requested_at,
        manual_wait_hours,
        hours_until_ready,
        effective_unmute_window_days,
        default_unmute_window_days,
        manual_fast_unmute_window_days
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
) AS mu ON tm.full_name = mu.full_name
    AND tm.branch = mu.branch
    AND tm.build_type = mu.build_type
LEFT JOIN (
    SELECT
        full_name AS full_name,
        branch AS branch,
        github_issue_url AS github_issue_url,
        github_issue_number AS github_issue_number,
        github_issue_state AS github_issue_state,
        github_issue_created_at AS github_issue_created_at
    FROM (
        SELECT
            g.*,
            ROW_NUMBER() OVER (PARTITION BY g.full_name, g.branch ORDER BY g.github_issue_created_at DESC, g.github_issue_number DESC) AS rn
        FROM `test_results/analytics/github_issue_mapping` AS g
    )
    WHERE rn = 1
) AS gim ON tm.full_name = gim.full_name
    AND tm.branch = gim.branch
