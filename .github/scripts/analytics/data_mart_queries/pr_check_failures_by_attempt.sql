-- PR-check: все падения тестов в окне lookback для пары (pr_number, branch) — и прошлые job'ы, и текущий last.
-- Разрез по attempt (1/2/3 из pull). Не rich-фильтр — любой failure.
--
-- is_last_run_in_pr: 1 если job_id = последний PR-check по (pr_number, branch) в окне
--   (MAX_BY(job_id, run_timestamp) по всем PR-check строкам, как в pr_blocked_by_failed_tests_rich), иначе 0.
--
-- Отличается от pr_with_test_failures / pr_blocked_by_tests:
--   там последний job исторически считался по строкам с attempt = 3; здесь last_job считается по всем attempt.
--
-- Строка = одно падение теста в attempt N на конкретном job_id.
-- В DataLens: фильтр is_last_run_in_pr = 1 — только last; без фильтра — вся история в окне.
-- Поля PR из github_data/pull_requests: pr_status, pr_state, pr_merged, pr_created_at (открыт),
-- pr_updated_at, pr_merged_at (влитие), pr_closed_at.
-- Mute-поля из tests_monitor:
--   mute_status_today и mute_status_in_run_date.
--
-- YDB: test_results/analytics/pr_check_failures_by_attempt
--
-- Parameters:
--   $pr_check_lookback_days — окно по run_timestamp (дни).

PRAGMA AnsiInForEmptyOrNullableItemsCollections;

$pr_check_lookback_days = 30;

$raw_pr_check = (
    SELECT
        job_id,
        run_timestamp,
        branch,
        build_type,
        suite_folder,
        test_name,
        suite_folder || '/' || test_name AS full_name,
        status,
        status_description,
        stderr,
        owners,
        log,
        logsdir,
        stdout,
        error_type,
        metadata,
        metrics,
        ListHead(
            Unicode::SplitToList(
                CASE
                    WHEN String::Contains(
                        ListHead(ListSkip(Unicode::SplitToList(CAST(pull AS Utf8), 'PR_'), 1)),
                        '#'
                    ) THEN ListHead(
                        ListSkip(
                            Unicode::SplitToList(
                                ListHead(ListSkip(Unicode::SplitToList(CAST(pull AS Utf8), 'PR_'), 1)),
                                '#'
                            ),
                            1
                        )
                    )
                    ELSE ListHead(ListSkip(Unicode::SplitToList(CAST(pull AS Utf8), 'PR_'), 1))
                END,
                '_'
            )
        ) AS pr_number,
        CASE
            WHEN String::Contains(pull, 'attempt_') THEN COALESCE(
                CAST(ListHead(Unicode::SplitToList(ListHead(ListSkip(Unicode::SplitToList(CAST(pull AS Utf8), 'attempt_'), 1)), '_')) AS Int32),
                1
            )
            WHEN String::Contains(pull, '_A') THEN COALESCE(
                CAST(ListHead(Unicode::SplitToList(ListHead(ListSkip(Unicode::SplitToList(CAST(pull AS Utf8), '_A'), 1)), '_')) AS Int32),
                1
            )
            ELSE 1
        END AS attempt_number
    FROM
        `test_results/test_runs_column`
    WHERE
        build_type = 'relwithdebinfo'
        AND job_name = 'PR-check'
        AND run_timestamp > CurrentUtcDate() - $pr_check_lookback_days * Interval("P1D")
        AND pull IS NOT NULL
        AND pull != ''
        AND String::Contains(pull, 'PR_')
        AND job_id IS NOT NULL
        AND branch IS NOT NULL
        AND suite_folder IS NOT NULL
        AND test_name IS NOT NULL
);

-- Один timestamp на (job_id, pr_number, branch) — как в pr_blocked_by_failed_tests_rich.$all_pr_check_runs
$job_ts = (
    SELECT
        job_id,
        pr_number,
        branch,
        MAX(run_timestamp) AS run_timestamp
    FROM
        $raw_pr_check
    GROUP BY
        job_id,
        pr_number,
        branch
);

$last_job_per_pr_branch = (
    SELECT
        pr_number,
        branch,
        MAX_BY(job_id, run_timestamp) AS last_job_id,
        MAX(run_timestamp) AS last_job_run_timestamp
    FROM
        $job_ts
    GROUP BY
        pr_number,
        branch
);

$pr_latest = (
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
        created_at,
        updated_at,
        merged_at,
        closed_at
    FROM (
        SELECT
            pr_number,
            base_ref_name,
            state,
            merged,
            title,
            url,
            created_at,
            updated_at,
            merged_at,
            closed_at,
            exported_at,
            created_date,
            ROW_NUMBER() OVER (PARTITION BY pr_number ORDER BY exported_at DESC, created_date DESC) AS rn
        FROM
            `github_data/pull_requests`
    ) AS ranked
    WHERE
        ranked.rn = 1
);

$monitor_today = (
    SELECT
        full_name AS m_full_name,
        branch AS m_branch,
        build_type AS m_build_type,
        is_muted
    FROM
        `test_results/analytics/tests_monitor`
    WHERE
        build_type = 'relwithdebinfo'
        AND date_window = CurrentUtcDate()
);

$monitor_run_day = (
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
        AND date_window >= CurrentUtcDate() - $pr_check_lookback_days * Interval("P1D")
        AND date_window <= CurrentUtcDate()
);

SELECT
    CAST(f.full_name AS String) AS full_name,
    CAST(f.suite_folder AS Utf8) AS suite_folder,
    CAST(f.test_name AS Utf8) AS test_name,
    CAST(COALESCE(f.pr_number, '') AS String) AS pr_number,
    CAST(COALESCE(f.job_id, 0) AS Uint64) AS job_id,
    CAST(
        COALESCE('https://github.com/ydb-platform/ydb/actions/runs/' || CAST(f.job_id AS UTF8), '') AS String
    ) AS run_url,
    f.run_timestamp AS run_timestamp,
    f.run_timestamp AS last_run_timestamp,
    CAST(f.branch AS Utf8) AS branch,
    CAST(COALESCE(f.build_type, 'relwithdebinfo') AS String) AS build_type,
    CAST(COALESCE(f.status, '') AS String) AS status,
    CAST(COALESCE(f.status_description, '') AS String) AS status_description,
    CAST(COALESCE(f.stderr, '') AS Utf8) AS stderr,
    CAST(COALESCE(f.attempt_number, 1) AS Int32) AS attempt_number,
    CAST(
        CASE
            WHEN lj.last_job_id IS NOT NULL AND f.job_id = lj.last_job_id THEN 1
            ELSE 0
        END AS Uint8
    ) AS is_last_run_in_pr,
    COALESCE(pr.base_ref_name, '') AS pr_target_branch,
    COALESCE(pr.pr_status, 'unknown') AS pr_status,
    COALESCE(pr.state, '') AS pr_state,
    pr.merged AS pr_merged,
    pr.title AS pr_title,
    pr.url AS pr_url,
    pr.created_at AS pr_created_at,
    pr.updated_at AS pr_updated_at,
    pr.merged_at AS pr_merged_at,
    pr.closed_at AS pr_closed_at,
    CAST(COALESCE(m.is_muted, 0) AS Uint8) AS mute_status_today,
    CAST(COALESCE(m_run.is_muted, 0) AS Uint8) AS mute_status_in_run_date,
    COALESCE(o.owners, f.owners) AS owners
FROM
    $raw_pr_check AS f
LEFT JOIN
    $last_job_per_pr_branch AS lj
ON
    f.pr_number = lj.pr_number
    AND f.branch = lj.branch
INNER JOIN
    $pr_latest AS pr
ON
    CAST(pr.pr_number AS Utf8) = f.pr_number
LEFT JOIN
    `test_results/analytics/testowners` AS o
ON
    o.suite_folder = f.suite_folder
    AND o.test_name = f.test_name
LEFT JOIN
    $monitor_today AS m
ON
    f.full_name = m.m_full_name
    AND f.branch = m.m_branch
    AND f.build_type = m.m_build_type
LEFT JOIN
    $monitor_run_day AS m_run
ON
    f.full_name = m_run.m_run_full_name
    AND f.branch = m_run.m_run_branch
    AND f.build_type = m_run.m_run_build_type
    AND m_run.date_window = CAST(f.run_timestamp AS Date)
WHERE
    f.status = 'failure'
    AND f.status != 'skipped'
ORDER BY
    f.run_timestamp DESC,
    f.pr_number,
    f.attempt_number,
    f.full_name;
