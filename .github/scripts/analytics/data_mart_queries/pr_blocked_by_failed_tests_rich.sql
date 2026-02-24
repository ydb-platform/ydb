-- Tests that failed in PR-check and "should pass" (stable in regression/nightly).
--
-- Logic:
--   1. Take all PR-check test failures in the last $pr_check_lookback_days days.
--   2. For each failed test, look at regression/nightly/postcommit runs
--      in the $regression_window_days days before the PR-check failure.
--   3. Keep only tests that in that window:
--      - have at least one passed in regression/nightly
--      - have no failed or mute
--   4. This filters out flaky tests — only those that are stable on main
--      but failed in the PR (likely due to PR changes) remain.
--   5. For each PR we take the latest PR-check run by time; only failures
--      from that last run are included (mute etc. are joined to these later).
--
-- Parameters:
--   $pr_check_lookback_days — window for PR-check failures (days). Default 7.
--   $regression_window_days — window for finding passed regression/nightly tests
--      relative to the PR-check failure time. Default 15.

PRAGMA AnsiInForEmptyOrNullableItemsCollections;

$pr_check_lookback_days = 1;  -- use 1 for one-day refresh
$regression_window_days = 4;

-- PR-check failures in the last $pr_check_lookback_days days (branch, full_name, run_timestamp)
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

-- Regression/postcommit runs with full_name precomputed (JOIN only on column equalities for YDB)
$regression_runs = (
    SELECT
        branch,
        suite_folder || '/' || test_name AS full_name,
        run_timestamp,
        status,
        job_id
    FROM
        `test_results/test_runs_column`
    WHERE
        build_type = 'relwithdebinfo'
        AND job_name IN (
            'Nightly-run',
            'Regression-run',
            'Regression-run_Large',
            'Regression-run_Small_and_Medium',
            'Regression-run_compatibility',
            'Regression-whitelist-run',
            'Postcommit_relwithdebinfo',
            'Postcommit_asan'
        )
        AND run_timestamp > CurrentUtcDate() - ($pr_check_lookback_days + $regression_window_days) * Interval("P1D")
        AND branch IS NOT NULL
        AND suite_folder IS NOT NULL
        AND test_name IS NOT NULL
);

-- For each (branch, full_name, run_ts) from PR-check: has regression passed in [run_ts - 15d, run_ts] and no failed/mute
$pr_check_with_regression_ok = (
    SELECT
        p.branch AS branch,
        p.full_name AS full_name,
        p.run_timestamp AS run_timestamp
    FROM
        $pr_check_failures_1d AS p
    INNER JOIN
        $regression_runs AS r
        ON r.branch = p.branch
        AND r.full_name = p.full_name
    WHERE
        r.run_timestamp >= p.run_timestamp - $regression_window_days * Interval("P1D")
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

-- Tests that passed the filter: failed in PR-check and have regression passed in the 15-day window before that run
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
        base.stderr AS stderr,
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
    -- All PR-check jobs (not only failures) so we can determine the latest PR run correctly
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
        f.stderr AS stderr,
        f.pr_number AS pr_number,
        f.attempt_number AS attempt_number,
        CASE WHEN f.job_id = l.last_job_id THEN 1 ELSE 0 END AS is_last_run_in_pr
    FROM 
        $all_failures_with_pr_base AS f
    LEFT JOIN
        $last_job_per_pr AS l
        ON f.pr_number = l.pr_number
);

-- Only failures from the latest PR-check run per PR that passed the regression check for that run
-- (branch, full_name, run_timestamp) must be in $pr_check_with_regression_ok
$failures_in_last_pr_run = (
    SELECT
        f.full_name AS full_name,
        f.suite_folder AS suite_folder,
        f.test_name AS test_name,
        f.pr_number AS pr_number,
        f.job_id AS job_id,
        f.run_timestamp AS run_timestamp,
        f.branch AS branch,
        f.status_description AS status_description,
        f.stderr AS stderr,
        f.attempt_number AS attempt_number
    FROM
        $all_failures_with_pr AS f
    INNER JOIN
        $pr_check_with_regression_ok AS ok
        ON ok.branch = f.branch
        AND ok.full_name = f.full_name
        AND ok.run_timestamp = f.run_timestamp
    WHERE
        f.pr_number IS NOT NULL
        AND f.job_id IS NOT NULL
        AND f.is_last_run_in_pr = 1
);

SELECT
    CAST(full_name AS String) AS full_name,
    CAST(suite_folder AS Utf8) AS suite_folder,
    CAST(test_name AS Utf8) AS test_name,
    CAST(COALESCE(pr_number, '0') AS String) AS pr_number,
    CAST(COALESCE(job_id, 0) AS Uint64) AS job_id,
    CAST(COALESCE('https://github.com/ydb-platform/ydb/actions/runs/' || CAST(job_id AS UTF8), 'FALLBACK_URL') AS String) AS run_url,
    run_timestamp AS last_run_timestamp,
    CAST(branch AS Utf8) AS branch,
    CAST('relwithdebinfo' AS String) AS build_type,
    CAST(COALESCE(status_description, '') AS String) AS status_description,
    CAST(COALESCE(stderr, '') AS String) AS stderr,
    CAST(COALESCE(attempt_number, 1) AS Int32) AS attempt_number,
    1 AS is_last_run_in_pr
FROM
    $failures_in_last_pr_run;