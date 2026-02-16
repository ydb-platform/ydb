-- PR blocked by failed tests (rich) + данные по PR и флаги мьюта.
--
-- Логика:
--   1. Берём данные из test_results/analytics/pr_blocked_by_failed_tests_rich
--      (тесты, упавшие в PR-check при стабильности в regression/nightly).
--   2. Джойним актуальное состояние PR из github_data/pull_requests
--      (последняя запись по pr_number: base_ref_name, state, merged, title, url и т.д.).
--   3. Джойним tests_monitor на «сегодня» (date_window = CurrentUtcDate())
--      — флаг is_muted_today и owner_today.
--   4. Джойним tests_monitor на день запуска (date_window = день last_run_timestamp)
--      — флаг is_muted_in_run_day.
--   5. Ограничиваем выборку последними $lookback_days днями по last_run_timestamp.
--
-- Параметры:
--   $lookback_days — окно выборки по дате прогона и по date_window в tests_monitor (дни). По умолчанию 30.

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
            full_name,
            branch,
            build_type,
            owner,
            is_muted
        FROM
            `test_results/analytics/tests_monitor`
        WHERE
            date_window = CurrentUtcDate()
    ) AS m
ON
    t.full_name = m.full_name
    AND t.branch = m.branch
    AND t.build_type = m.build_type
LEFT JOIN
    (
        SELECT
            full_name,
            branch,
            build_type,
            date_window,
            is_muted
        FROM
            `test_results/analytics/tests_monitor`
        WHERE
            date_window >= CurrentUtcDate() - $lookback_days * Interval("P1D")
            AND date_window <= CurrentUtcDate()
    ) AS m_run
ON
    t.full_name = m_run.full_name
    AND t.branch = m_run.branch
    AND t.build_type = m_run.build_type
    AND m_run.date_window = CAST(t.last_run_timestamp AS Date)
WHERE
    t.last_run_timestamp > CurrentUtcDate() - $lookback_days * Interval("P1D")
ORDER BY 
    last_run_timestamp DESC,
    pr_number,
    full_name
