-- Тесты, которые упали в PR-check и "должны проходить" (стабильны в regression/nightly).
--
-- Логика:
--   1. Берём все падения тестов в PR-check за последние $pr_check_lookback_days дней.
--   2. Для каждого упавшего теста смотрим regression/nightly/postcommit прогоны
--      за $regression_window_days дней ДО момента падения в PR-check.
--   3. Оставляем только тесты, которые в этом окне:
--      - имеют хотя бы один passed в regression/nightly
--      - НЕ имеют failed или mute
--   4. Это фильтрует "флакающие" тесты — остаются только те, что стабильно проходят
--      в основной ветке, но упали в PR (вероятно, из-за изменений в PR).
--
-- Параметры:
--   $pr_check_lookback_days — окно выборки PR-check failures (дни). По умолчанию 7.
--   $regression_window_days — окно для поиска passed regression/nightly тестов
--      относительно времени падения в PR-check. По умолчанию 15.

PRAGMA AnsiInForEmptyOrNullableItemsCollections;

$pr_check_lookback_days = 7;
$regression_window_days = 15;

-- PR-check failures за последние $pr_check_lookback_days дней (branch, full_name, run_timestamp)
$pr_check_failures_1d = (
    SELECT
        branch,
        suite_folder || '/' || test_name AS full_name,
        run_timestamp
    FROM
        `test_results/test_runs_column`
    WHERE
        build_type = 'relwithdebinfo'
        AND job_name = 'PR-check'
        AND status = 'failure'
        AND run_timestamp > CurrentUtcDate() - $pr_check_lookback_days * Interval("P1D")
        AND pull IS NOT NULL
        AND pull != ''
        AND String::Contains(pull, 'PR_')
        AND job_id IS NOT NULL
        AND branch IS NOT NULL
        AND suite_folder IS NOT NULL
        AND test_name IS NOT NULL
    GROUP BY
        branch,
        suite_folder,
        test_name,
        run_timestamp
);

-- Для каждого (branch, full_name, run_ts) из PR-check: есть ли regression passed в [run_ts - 15d, run_ts] и нет failed/mute
$pr_check_with_regression_ok = (
    SELECT
        p.branch AS branch,
        p.full_name AS full_name,
        p.run_timestamp AS run_timestamp
    FROM
        $pr_check_failures_1d AS p
    INNER JOIN
        `test_results/test_runs_column` AS r
        ON r.branch = p.branch
        AND r.suite_folder || '/' || r.test_name = p.full_name
    WHERE
        r.build_type = 'relwithdebinfo'
        AND r.job_name IN (
            'Nightly-run',
            'Regression-run',
            'Regression-run_Large',
            'Regression-run_Small_and_Medium',
            'Regression-run_compatibility',
            'Regression-whitelist-run',
            'Postcommit_relwithdebinfo',
            'Postcommit_asan'
        )
        AND r.run_timestamp >= p.run_timestamp - $regression_window_days * Interval("P1D")
        AND r.run_timestamp <= p.run_timestamp
    GROUP BY
        p.branch,
        p.full_name,
        p.run_timestamp
    HAVING
        COUNT(DISTINCT CASE WHEN r.status = 'passed' THEN r.job_id ELSE NULL END) > 0
        AND COUNT(DISTINCT CASE WHEN r.status != 'passed' AND r.status != 'skipped' AND r.status != 'mute' THEN r.job_id ELSE NULL END) = 0
        AND COUNT(DISTINCT CASE WHEN r.status = 'mute' THEN r.job_id ELSE NULL END) = 0
);

-- Тесты, которые прошли фильтр: падали в PR-check (1 день) и по времени своего PR-check run имеют regression passed в окне 15 дней назад
$filtered_tests = (
    SELECT DISTINCT
        branch,
        full_name
    FROM
        $pr_check_with_regression_ok
);

$all_failures_with_pr_base = (
    SELECT 
        base.suite_folder || '/' || base.test_name AS full_name,
        base.suite_folder AS suite_folder,
        base.test_name AS test_name,
        base.job_id AS job_id,
        base.run_timestamp AS run_timestamp,
        base.branch AS branch,
        base.status_description AS status_description,
        ListHead(
            Unicode::SplitToList(
                CASE 
                    WHEN String::Contains(
                        ListHead(ListSkip(Unicode::SplitToList(CAST(base.pull AS UTF8), 'PR_'), 1)),
                        '#'
                    ) THEN ListHead(
                        ListSkip(
                            Unicode::SplitToList(
                                ListHead(ListSkip(Unicode::SplitToList(CAST(base.pull AS UTF8), 'PR_'), 1)),
                                '#'
                            ),
                            1
                        )
                    )
                    ELSE ListHead(ListSkip(Unicode::SplitToList(CAST(base.pull AS UTF8), 'PR_'), 1))
                END,
                '_'
            )
        ) AS pr_number,
        CASE 
            WHEN String::Contains(base.pull, 'attempt_') THEN COALESCE(CAST(ListHead(Unicode::SplitToList(ListHead(ListSkip(Unicode::SplitToList(CAST(base.pull AS UTF8), 'attempt_'), 1)), '_')) AS Int32), 1)
            WHEN String::Contains(base.pull, '_A') THEN COALESCE(CAST(ListHead(Unicode::SplitToList(ListHead(ListSkip(Unicode::SplitToList(CAST(base.pull AS UTF8), '_A'), 1)), '_')) AS Int32), 1)
            ELSE 1
        END AS attempt_number
    FROM 
        `test_results/test_runs_column` AS base
    INNER JOIN
        $filtered_tests AS ft
        ON ft.branch = base.branch
        AND ft.full_name = base.suite_folder || '/' || base.test_name
    WHERE 
        base.build_type = 'relwithdebinfo'
        AND base.status != 'skipped'
        AND base.job_name = 'PR-check'
        AND base.status = 'failure'
        AND base.run_timestamp > CurrentUtcDate() - $pr_check_lookback_days * Interval("P1D")
        AND base.pull IS NOT NULL
        AND base.pull != ''
        AND String::Contains(base.pull, 'PR_')
        AND base.job_id IS NOT NULL
        AND base.branch IS NOT NULL
        AND base.suite_folder IS NOT NULL
        AND base.test_name IS NOT NULL
);

$all_pr_check_runs = (
    -- Все PR-check job'ы (не только failure), чтобы корректно определить последний запуск PR
    SELECT
        job_id,
        pr_number,
        MAX(run_timestamp) AS run_timestamp
    FROM (
        SELECT
            job_id,
            run_timestamp,
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
            ) AS pr_number
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
    ) AS runs
    GROUP BY
        job_id,
        pr_number
);

$last_job_per_pr = (
    SELECT
        pr_number,
        MAX_BY(job_id, run_timestamp) AS last_job_id
    FROM
        $all_pr_check_runs
    GROUP BY
        pr_number
);

$all_failures_with_pr = (
    SELECT 
        f.full_name AS full_name,
        f.suite_folder AS suite_folder,
        f.test_name AS test_name,
        f.job_id AS job_id,
        f.run_timestamp AS run_timestamp,
        f.branch AS branch,
        f.status_description AS status_description,
        f.pr_number AS pr_number,
        f.attempt_number AS attempt_number,
        CASE WHEN f.job_id = l.last_job_id THEN 1 ELSE 0 END AS is_last_run_in_pr
    FROM 
        $all_failures_with_pr_base AS f
    LEFT JOIN
        $last_job_per_pr AS l
        ON f.pr_number = l.pr_number
);

$test_pr_failures = (
    SELECT 
        full_name,
        suite_folder,
        test_name,
        pr_number,
        job_id,
        branch,
        MAX(run_timestamp) AS last_run_timestamp,
        MAX_BY(status_description, run_timestamp) AS status_description,
        MAX_BY(attempt_number, run_timestamp) AS attempt_number,
        MAX_BY(is_last_run_in_pr, run_timestamp) AS is_last_run_in_pr
    FROM 
        $all_failures_with_pr
    WHERE 
        pr_number IS NOT NULL
        AND job_id IS NOT NULL
    GROUP BY 
        full_name,
        suite_folder,
        test_name,
        pr_number,
        job_id,
        branch
);

$last_run_per_test_pr = (
    SELECT 
        full_name,
        suite_folder,
        test_name,
        pr_number,
        job_id,
        branch,
        last_run_timestamp,
        status_description,
        attempt_number,
        is_last_run_in_pr,
        ROW_NUMBER() OVER (
            PARTITION BY full_name, pr_number, branch
            ORDER BY last_run_timestamp DESC, job_id DESC
        ) AS rn
    FROM 
        $test_pr_failures
);

SELECT 
    CAST(full_name AS String) AS full_name,
    CAST(suite_folder AS Utf8) AS suite_folder,
    CAST(test_name AS Utf8) AS test_name,
    CAST(COALESCE(pr_number, '0') AS String) AS pr_number,
    CAST(COALESCE(job_id, 0) AS Uint64) AS job_id,
    CAST(COALESCE('https://github.com/ydb-platform/ydb/actions/runs/' || CAST(job_id AS UTF8), 'FALLBACK_URL') AS String) AS run_url,
    last_run_timestamp,
    CAST(branch AS Utf8) AS branch,
    CAST('relwithdebinfo' AS String) AS build_type,
    CAST(COALESCE(status_description, '') AS String) AS status_description,
    CAST(COALESCE(attempt_number, 1) AS Int32) AS attempt_number,
    CAST(COALESCE(is_last_run_in_pr, 0) AS Int32) AS is_last_run_in_pr
FROM 
    $last_run_per_test_pr
WHERE 
    rn = 1;