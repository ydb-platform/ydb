-- PR blocked by failed tests (rich) + PR data and mute flags.
--
-- Logic:
--   1. Take data from test_results/analytics/pr_blocked_by_failed_tests_rich
--      (tests that failed in PR-check while stable in regression/nightly).
--   2. Join current PR state from github_data/pull_requests
--      (latest row per pr_number: base_ref_name, state, merged, title, url, etc.).
--   3. Join tests_monitor for "today" (date_window = CurrentUtcDate())
--      — is_muted_today and owner_today.
--   4. Join tests_monitor for the run day (date_window = day of last_run_timestamp)
--      — is_muted_in_run_day.
--   5. Restrict the result set to the last $lookback_days by last_run_timestamp.
--
-- Mute rule checks (counts, met_mute_criteria, raw vs monitor, JSON) are done in
-- pr_failed_tests_validation_through_mute_rules.sql (all PR-check failures there, not only "blocked").

$lookback_days = 1;

SELECT 
    t.full_name AS full_name,
    t.suite_folder AS suite_folder,
    t.test_name AS test_name,
    t.pr_number AS pr_number,
    t.job_id AS job_id,
    t.run_url AS run_url,
    t.last_run_timestamp AS last_run_timestamp,
    t.branch AS branch,
    t.build_type AS build_type,
    t.status_description AS status_description,
    t.stderr AS stderr,
    t.attempt_number AS attempt_number,
    t.is_last_run_in_pr AS is_last_run_in_pr,
    COALESCE(pr.base_ref_name, '') AS pr_target_branch,
    COALESCE(pr.pr_status, 'unknown') AS pr_status,
    COALESCE(pr.state, '') AS pr_state,
    pr.merged AS pr_merged,
    pr.title AS pr_title,
    pr.url AS pr_url,
    pr.merged_at AS pr_merged_at,
    pr.closed_at AS pr_closed_at,
    CAST(COALESCE(m.is_muted, 0) AS Uint8) AS is_muted_today,
    CAST(COALESCE(m_run.is_muted, 0) AS Uint8) AS is_muted_in_run_day,
    COALESCE(m.owner, '') AS owner_today
FROM 
    `test_results/analytics/pr_blocked_by_failed_tests_rich` AS t
LEFT JOIN 
    (
        SELECT 
            pr_number,
            base_ref_name,
            state,
            merged,
            CASE 
                WHEN merged = 1 THEN 'merged'
                WHEN state = 'OPEN' THEN 'open'
                WHEN state = 'CLOSED' THEN 'closed'
                ELSE COALESCE(state, 'unknown')
            END AS pr_status,
            title,
            url,
            merged_at,
            closed_at
        FROM 
            (
                SELECT 
                    pr_number,
                    base_ref_name,
                    state,
                    merged,
                    title,
                    url,
                    merged_at,
                    closed_at,
                    ROW_NUMBER() OVER (PARTITION BY pr_number ORDER BY exported_at DESC, created_date DESC) AS rn
                FROM 
                    `github_data/pull_requests`
            ) AS ranked
        WHERE 
            ranked.rn = 1
    ) AS pr
ON 
    CAST(t.pr_number AS Uint64) = pr.pr_number
LEFT JOIN
    (
        SELECT
            full_name AS m_full_name,
            branch AS m_branch,
            build_type AS m_build_type,
            owner,
            is_muted
        FROM
            `test_results/analytics/tests_monitor`
        WHERE
            build_type = 'relwithdebinfo'
            AND date_window = CurrentUtcDate()
    ) AS m
ON
    t.full_name = m.m_full_name
    AND t.branch = m.m_branch
    AND t.build_type = m.m_build_type
LEFT JOIN
    (
        SELECT
            full_name AS m_run_full_name,
            branch AS m_run_branch,
            build_type AS m_run_build_type,
            date_window,
            is_muted
        FROM
            `test_results/analytics/tests_monitor`
        WHERE
            build_type = 'relwithdebinfo'
            AND date_window >= CurrentUtcDate() - $lookback_days * Interval("P1D")
            AND date_window <= CurrentUtcDate()
    ) AS m_run
ON
    t.full_name = m_run.m_run_full_name
    AND t.branch = m_run.m_run_branch
    AND t.build_type = m_run.m_run_build_type
    AND m_run.date_window = CAST(t.last_run_timestamp AS Date)
WHERE
    t.last_run_timestamp > CurrentUtcDate() - $lookback_days * Interval("P1D")
ORDER BY 
    t.last_run_timestamp DESC,
    t.pr_number,
    t.full_name
