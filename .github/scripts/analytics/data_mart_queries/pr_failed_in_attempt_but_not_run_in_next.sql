-- PR и тесты: failure в attempt N, но тест не запускался в attempt N+1 (в рамках одного job_id).
--
-- В одном PR может быть несколько запусков (разные коммиты → разные job_id).
-- В рамках одного job_id бывают попытки 1, 2, 3. Сравниваем только внутри одного job_id.
--
-- Логика:
--   1. Берём все PR-check записи за $lookback_days дней с pr_number и attempt_number.
--   2. По каждому job_id: тесты, которые запускались в attempt 2; тесты, которые запускались в attempt 3.
--   3. "Пропавшими" считаем только если следующая попытка вообще выполнялась (есть записи в базе):
--      — job_id входит в множество job_id, для которых есть хотя бы одна запись attempt 2 / attempt 3.
--      Иначе прерванный запуск (нет попытки 2 вообще) не считаем "тест не запустился в attempt 2".
--   4. Оставляем только:
--      - failure в attempt 1 при отсутствии запуска этого теста в attempt 2 в том же job_id, ИЛИ
--      - failure в attempt 2 при отсутствии запуска этого теста в attempt 3 в том же job_id.
--   5. Джойним актуальное состояние PR из github_data/pull_requests.
--
-- Параметры:
--   $lookback_days — окно выборки по run_timestamp (дни). По умолчанию 30.

PRAGMA AnsiInForEmptyOrNullableItemsCollections;

$lookback_days = 30;

-- Все PR-check записи с pr_number и attempt_number за окно
$all_runs = (
    SELECT
        branch,
        suite_folder || '/' || test_name AS full_name,
        job_id,
        run_timestamp,
        status,
        status_description,
        suite_folder,
        test_name,
        ListHead(
            Unicode::SplitToList(
                CASE
                    WHEN String::Contains(
                        ListHead(ListSkip(Unicode::SplitToList(CAST(pull AS UTF8), 'PR_'), 1)),
                        '#'
                    ) THEN ListHead(
                        ListSkip(
                            Unicode::SplitToList(
                                ListHead(ListSkip(Unicode::SplitToList(CAST(pull AS UTF8), 'PR_'), 1)),
                                '#'
                            ),
                            1
                        )
                    )
                    ELSE ListHead(ListSkip(Unicode::SplitToList(CAST(pull AS UTF8), 'PR_'), 1))
                END,
                '_'
            )
        ) AS pr_number,
        CASE
            WHEN String::Contains(pull, 'attempt_') THEN COALESCE(CAST(ListHead(Unicode::SplitToList(ListHead(ListSkip(Unicode::SplitToList(CAST(pull AS UTF8), 'attempt_'), 1)), '_')) AS Int32), 1)
            WHEN String::Contains(pull, '_A') THEN COALESCE(CAST(ListHead(Unicode::SplitToList(ListHead(ListSkip(Unicode::SplitToList(CAST(pull AS UTF8), '_A'), 1)), '_')) AS Int32), 1)
            ELSE 1
        END AS attempt_number
    FROM
        `test_results/test_runs_column`
    WHERE
        build_type = 'relwithdebinfo'
        AND job_name = 'PR-check'
        AND run_timestamp > CurrentUtcDate() - $lookback_days * Interval("P1D")
        AND pull IS NOT NULL
        AND pull != ''
        AND String::Contains(pull, 'PR_')
        AND job_id IS NOT NULL
        AND branch IS NOT NULL
        AND suite_folder IS NOT NULL
        AND test_name IS NOT NULL
);

-- Job_id, для которых в базе есть хотя бы одна запись о attempt 2 (попытка 2 выполнялась, возможно прервана)
$jobs_with_attempt2 = (
    SELECT DISTINCT job_id
    FROM $all_runs
    WHERE attempt_number = 2
);

-- Job_id, для которых в базе есть хотя бы одна запись о attempt 3
$jobs_with_attempt3 = (
    SELECT DISTINCT job_id
    FROM $all_runs
    WHERE attempt_number = 3
);

-- Тесты, которые запускались в attempt 2 в рамках того же job_id (job_id, branch, full_name)
$ran_in_attempt2 = (
    SELECT DISTINCT
        job_id,
        branch,
        full_name
    FROM
        $all_runs
    WHERE
        attempt_number = 2
);

-- Тесты, которые запускались в attempt 3 в рамках того же job_id
$ran_in_attempt3 = (
    SELECT DISTINCT
        job_id,
        branch,
        full_name
    FROM
        $all_runs
    WHERE
        attempt_number = 3
);

-- Failure в attempt 1, для которых в том же job_id нет запуска в attempt 2.
-- Учитываем только job_id, где attempt 2 вообще есть в базе (иначе это прерванный запуск, а не "тест не запустился").
$failed_1_not_run_2 = (
    SELECT
        r.full_name AS full_name,
        r.suite_folder AS suite_folder,
        r.test_name AS test_name,
        r.pr_number AS pr_number,
        r.job_id AS job_id,
        r.run_timestamp AS run_timestamp,
        r.branch AS branch,
        r.status_description AS status_description,
        r.attempt_number AS attempt_number,
        1 AS gap_type
    FROM
        $all_runs AS r
    INNER JOIN
        $jobs_with_attempt2 AS j2
        ON j2.job_id = r.job_id
    LEFT JOIN
        $ran_in_attempt2 AS a2
        ON a2.job_id = r.job_id
        AND a2.branch = r.branch
        AND a2.full_name = r.full_name
    WHERE
        r.status = 'failure'
        AND r.attempt_number = 1
        AND a2.job_id IS NULL
);

-- Failure в attempt 2, для которых в том же job_id нет запуска в attempt 3.
-- Учитываем только job_id, где attempt 3 вообще есть в базе.
$failed_2_not_run_3 = (
    SELECT
        r.full_name AS full_name,
        r.suite_folder AS suite_folder,
        r.test_name AS test_name,
        r.pr_number AS pr_number,
        r.job_id AS job_id,
        r.run_timestamp AS run_timestamp,
        r.branch AS branch,
        r.status_description AS status_description,
        r.attempt_number AS attempt_number,
        2 AS gap_type
    FROM
        $all_runs AS r
    INNER JOIN
        $jobs_with_attempt3 AS j3
        ON j3.job_id = r.job_id
    LEFT JOIN
        $ran_in_attempt3 AS a3
        ON a3.job_id = r.job_id
        AND a3.branch = r.branch
        AND a3.full_name = r.full_name
    WHERE
        r.status = 'failure'
        AND r.attempt_number = 2
        AND a3.job_id IS NULL
);

$gaps = (
    SELECT * FROM $failed_1_not_run_2
    UNION ALL
    SELECT * FROM $failed_2_not_run_3
);

-- Один представитель по (full_name, pr_number, branch, gap_type) — последний run_timestamp
$gaps_dedup = (
    SELECT
        full_name,
        suite_folder,
        test_name,
        pr_number,
        branch,
        MAX_BY(job_id, run_timestamp) AS job_id,
        MAX(run_timestamp) AS last_run_timestamp,
        MAX_BY(status_description, run_timestamp) AS status_description,
        MAX_BY(attempt_number, run_timestamp) AS attempt_number,
        gap_type
    FROM
        $gaps
    GROUP BY
        full_name,
        suite_folder,
        test_name,
        pr_number,
        branch,
        gap_type
);

SELECT
    g.full_name AS full_name,
    g.suite_folder AS suite_folder,
    g.test_name AS test_name,
    CAST(CASE WHEN String::Contains(g.full_name, ' chunk') THEN 1 ELSE 0 END AS Uint8) AS is_test_chunk,
    g.pr_number AS pr_number,
    g.job_id AS job_id,
    CAST('https://github.com/ydb-platform/ydb/actions/runs/' || CAST(g.job_id AS UTF8) AS String) AS run_url,
    g.last_run_timestamp AS last_run_timestamp,
    g.branch AS branch,
    CAST('relwithdebinfo' AS String) AS build_type,
    g.status_description AS status_description,
    g.attempt_number AS attempt_number,
    g.gap_type AS gap_type,
    COALESCE(pr.base_ref_name, '') AS pr_target_branch,
    COALESCE(pr.pr_status, 'unknown') AS pr_status,
    COALESCE(pr.state, '') AS pr_state,
    pr.merged AS pr_merged,
    pr.title AS pr_title,
    pr.url AS pr_url,
    pr.merged_at AS pr_merged_at,
    pr.closed_at AS pr_closed_at
FROM
    $gaps_dedup AS g
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
    CAST(g.pr_number AS Uint64) = pr.pr_number
ORDER BY
    g.last_run_timestamp DESC,
    g.pr_number,
    g.full_name
;
