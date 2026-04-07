-- Все запуски по последнему коммиту для merged PR из марты.
--
-- Логика:
--   1. По каждому (pr_number, branch) берём один последний запуск — job с max(last_run_timestamp).
--   2. По этому job_id из test_runs_column получаем commit этого запуска.
--   3. По (commit, branch) выбираем в test_runs_column все запуски PR-check в эту ветку.
--   4. Джойним с мартой по (full_name, pr_number, branch) для полей марты.
--
-- Итог: только merged PR, по коммиту последнего job'а — все job'ы с тем же коммитом в той же ветке.
-- Выгружаем только те PR, у которых на последнем коммите было более одного job'а.
--
-- Параметры:
--   $lookback_days — окно по last_run_timestamp в марте (дни). По умолчанию 15.
--   Ограничиваем чтение test_runs_column по run_timestamp, иначе запрос даёт channel spilling (20Gb+).

$lookback_days = 10;
$run_ts_cutoff = $lookback_days + 10;  -- окно для скана runs (коммиты из марты не старше $lookback_days, runs — с запасом)

-- Один последний запуск на (pr_number, branch): job с максимальным last_run_timestamp
$last_run_jobs = (
    SELECT
        pr_number AS pr_number,
        branch AS branch,
        MAX_BY(job_id, last_run_timestamp) AS job_id
    FROM
        `test_results/analytics/pr_blocked_by_failed_tests_rich_with_pr_and_mute`
    WHERE
        pr_merged = 1
        AND last_run_timestamp > CurrentUtcDate() - $lookback_days * Interval("P1D")
        AND branch IN ('main')
        --, 'stable-26-1', 'stable-25-4', 'stable-25-4-1', 'stable-25-3-1', 'stable-26-1-1')
        AND job_id IS NOT NULL
        AND branch IS NOT NULL
        AND pr_number IS NOT NULL
    GROUP BY
        pr_number,
        branch
);

-- Для каждого такого job'а берём commit из test_runs_column
$job_commit = (
    SELECT
        j.pr_number AS pr_number,
        j.branch AS branch,
        j.job_id AS last_job_id,
        MAX_BY(r.commit, r.run_timestamp) AS commit_sha
    FROM
        $last_run_jobs AS j
    INNER JOIN
        `test_results/test_runs_column` AS r
        ON r.job_id = j.job_id AND r.branch = j.branch
    WHERE
        r.run_timestamp > CurrentUtcDate() - $run_ts_cutoff * Interval("P1D")
        AND r.commit IS NOT NULL
        AND r.commit != ''
        and r.job_name = 'PR-check'
        and r.branch in ('main')
        --, 'stable-26-1', 'stable-25-3', 'stable-25-4', 'stable-25-4-1', 'stable-25-3-1', 'stable-26-1-1')
    GROUP BY
        j.pr_number,
        j.branch,
        j.job_id
);

-- Все запуски PR-check по этим (commit_sha, branch) с извлечением pr_number из pull
$runs_with_pr = (
    SELECT
        r.suite_folder AS suite_folder,
        r.test_name AS test_name,
        r.suite_folder || '/' || r.test_name AS full_name,
        r.run_timestamp AS run_timestamp,
        r.job_id AS job_id,
        r.branch AS branch,
        r.build_type AS build_type,
        r.status AS status,
        r.status_description AS status_description,
        r.stderr AS stderr,
        r.commit AS commit_sha,
        c.pr_number AS pr_number,
        CASE
            WHEN String::Contains(r.pull, 'attempt_') THEN COALESCE(CAST(ListHead(Unicode::SplitToList(ListHead(ListSkip(Unicode::SplitToList(CAST(r.pull AS UTF8), 'attempt_'), 1)), '_')) AS Int32), 1)
            WHEN String::Contains(r.pull, '_A') THEN COALESCE(CAST(ListHead(Unicode::SplitToList(ListHead(ListSkip(Unicode::SplitToList(CAST(r.pull AS UTF8), '_A'), 1)), '_')) AS Int32), 1)
            ELSE 1
        END AS attempt_number
    FROM
        `test_results/test_runs_column` AS r
    INNER JOIN
        $job_commit AS c
        ON r.commit = c.commit_sha AND r.branch = c.branch
    WHERE
        r.run_timestamp > CurrentUtcDate() - $run_ts_cutoff * Interval("P1D")
        and r.branch in ('main')
        and r.build_type = 'relwithdebinfo'
        AND r.job_name = 'PR-check'
        AND r.pull IS NOT NULL
        
        AND r.pull != ''
);

-- Только те (pr_number, branch), у которых на последнем коммите больше одного job'а
$pr_with_multiple_jobs = (
    SELECT
        pr_number AS pr_number,
        branch AS branch
    FROM
        $runs_with_pr
    GROUP BY
        pr_number,
        branch
    HAVING
        COUNT(DISTINCT job_id) > 1
);

-- Джойн с мартой: к каждому запуску добавляем поля марты (по full_name, pr_number, branch)
SELECT
    run.full_name AS full_name,
    run.suite_folder AS suite_folder,
    run.test_name AS test_name,
    run.pr_number AS pr_number,
    run.job_id AS job_id,
    CAST(COALESCE('https://github.com/ydb-platform/ydb/actions/runs/' || CAST(run.job_id AS UTF8), '') AS String) AS run_url,
    run.run_timestamp AS run_timestamp,
    run.branch AS branch,
    run.build_type AS build_type,
    run.status AS status,
    COALESCE(run.status_description, '') AS status_description,
    COALESCE(run.stderr, '') AS stderr,
    run.commit_sha AS commit_sha,
    run.attempt_number AS attempt_number,
    COALESCE(m.pr_target_branch, '') AS pr_target_branch,
    COALESCE(m.pr_status, 'unknown') AS pr_status,
    COALESCE(m.pr_state, '') AS pr_state,
    m.pr_merged AS pr_merged,
    COALESCE(m.pr_title, '') AS pr_title,
    COALESCE(m.pr_url, '') AS pr_url,
    m.pr_merged_at AS pr_merged_at,
    m.pr_closed_at AS pr_closed_at,
    CAST(COALESCE(m.is_muted_today, 0) AS Uint8) AS is_muted_today,
    CAST(COALESCE(m.is_muted_in_run_day, 0) AS Uint8) AS is_muted_in_run_day,
    COALESCE(m.owner_today, '') AS owner_today,
    m.last_run_timestamp AS mart_last_run_timestamp,
    m.job_id AS mart_last_job_id,
    m.is_last_run_in_pr AS is_last_run_in_pr
FROM
    $runs_with_pr AS run
INNER JOIN
    $pr_with_multiple_jobs AS multi
    ON run.pr_number = multi.pr_number AND run.branch = multi.branch
LEFT JOIN
    (
        SELECT
            full_name AS full_name,
            pr_number AS pr_number,
            branch AS branch,
            pr_target_branch AS pr_target_branch,
            pr_status AS pr_status,
            pr_state AS pr_state,
            pr_merged AS pr_merged,
            pr_title AS pr_title,
            pr_url AS pr_url,
            pr_merged_at AS pr_merged_at,
            pr_closed_at AS pr_closed_at,
            is_muted_today AS is_muted_today,
            is_muted_in_run_day AS is_muted_in_run_day,
            owner_today AS owner_today,
            last_run_timestamp AS last_run_timestamp,
            job_id AS job_id,
            is_last_run_in_pr AS is_last_run_in_pr
        FROM
            `test_results/analytics/pr_blocked_by_failed_tests_rich_with_pr_and_mute`
        WHERE
            pr_merged = 1
             and branch in ('main')
            AND last_run_timestamp > CurrentUtcDate() - $lookback_days * Interval("P1D")
    ) AS m
    ON m.full_name = run.full_name
    AND m.pr_number = run.pr_number
    AND m.branch = run.branch
ORDER BY
    run.pr_number,
    run.branch,
    run.full_name,
    run.run_timestamp DESC;
