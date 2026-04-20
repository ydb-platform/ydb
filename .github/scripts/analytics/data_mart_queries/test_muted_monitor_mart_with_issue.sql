-- GitHub issue fields from github_issue_mapping; analytics area/owner + owner hand-off from tests_monitor.
-- COALESCE fallback: when effective_* columns are NULL (not yet populated), fall back to owner string + area_to_owner_mapping.

$normalize = ($raw_area) -> {
    $parts = String::SplitToList(Cast($raw_area AS String), '/');
    RETURN Cast(
        IF(ListLength($parts) >= 2, $parts[0] || '/' || $parts[1], Cast($raw_area AS String))
    AS Utf8);
};

$area_fallback = (
    SELECT owner_team AS owner_team, MIN($normalize(area)) AS area
    FROM `test_results/analytics/area_to_owner_mapping`
    GROUP BY owner_team
);

$gim_latest = (
    SELECT
        full_name AS full_name,
        branch AS branch,
        build_type AS build_type,
        github_issue_url AS github_issue_url,
        github_issue_number AS github_issue_number,
        github_issue_state AS github_issue_state,
        github_issue_created_at AS github_issue_created_at,
        area_override AS area_override,
        area_override_since AS area_override_since
    FROM (
        SELECT
            g.full_name AS full_name,
            g.branch AS branch,
            g.build_type AS build_type,
            g.github_issue_url AS github_issue_url,
            g.github_issue_number AS github_issue_number,
            g.github_issue_state AS github_issue_state,
            g.github_issue_created_at AS github_issue_created_at,
            g.area_override AS area_override,
            g.area_override_since AS area_override_since,
            ROW_NUMBER() OVER (
                PARTITION BY g.full_name, g.branch, g.build_type
                ORDER BY g.github_issue_created_at DESC, g.github_issue_number DESC
            ) AS rn
        FROM `test_results/analytics/github_issue_mapping` AS g
    ) AS g_rnk
    WHERE g_rnk.rn = 1
);

$mfu = (
    SELECT
        full_name AS full_name,
        branch AS branch,
        build_type AS build_type,
        github_issue_number AS mfu_issue_number,
        requested_at AS mfu_since,
        window_days AS mfu_window_days,
        requested_at + 2 * window_days * Interval("P1D") AS mfu_expires_at
    FROM `test_mute/fast_unmute_active`
);

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
    CAST(CASE
        WHEN (tm.state = 'Skipped' AND tm.days_in_state > 14) THEN 'Skipped'
        WHEN tm.days_in_mute_state > 30 THEN 'MUTED: delete candidate'
        ELSE 'MUTED: in sla' 
    END
    as String) AS resolution,
    CAST(Coalesce(
        tm.effective_owner_team,
        Unicode::ToLower(Cast(Coalesce(String::ReplaceAll(tm.owner, 'TEAM:@ydb-platform/', ''), '') AS Utf8))
    ) AS String) AS owner_team,
    Coalesce(tm.effective_area, $normalize(Coalesce(af.area, 'area/-'))) AS area,
    tm.previous_effective_owner_team AS previous_effective_owner_team,
    tm.effective_owner_team_changed_date AS effective_owner_team_changed_date,
    CAST(
        CASE
            WHEN tm.is_muted = 1 OR (tm.state = 'Skipped' AND tm.days_in_state > 14) THEN TRUE
            ELSE FALSE
        END AS Uint8
    ) AS is_muted_or_skipped,
    gim.github_issue_url AS github_issue_url,
    gim.github_issue_number AS github_issue_number,
    gim.github_issue_state AS github_issue_state,
    gim.github_issue_created_at AS github_issue_created_at,
    gim.area_override AS area_override,
    gim.area_override_since AS area_override_since,
    CAST(CASE WHEN mfu.full_name IS NOT NULL THEN 1 ELSE 0 END AS Uint8) AS is_manual_fast_unmute,
    mfu.mfu_since AS manual_fast_unmute_since,
    mfu.mfu_window_days AS manual_fast_unmute_window_days,
    mfu.mfu_expires_at AS manual_fast_unmute_expires_at,
    mfu.mfu_issue_number AS manual_fast_unmute_issue_number
FROM `test_results/analytics/tests_monitor` AS tm
LEFT JOIN $area_fallback AS af
    ON Unicode::ToLower(Cast(Coalesce(String::ReplaceAll(tm.owner, 'TEAM:@ydb-platform/', ''), '') AS Utf8)) = af.owner_team
LEFT JOIN $gim_latest AS gim
    ON tm.full_name = gim.full_name
    AND tm.branch = gim.branch
    AND tm.build_type = gim.build_type
LEFT JOIN $mfu AS mfu
    ON tm.full_name = mfu.full_name
    AND tm.branch = mfu.branch
    AND tm.build_type = mfu.build_type
WHERE tm.date_window >= CurrentUtcDate() - 1 * Interval("P1D")
    AND (tm.branch = 'main' OR tm.branch LIKE 'stable-%')
    AND tm.is_test_chunk = 0;
