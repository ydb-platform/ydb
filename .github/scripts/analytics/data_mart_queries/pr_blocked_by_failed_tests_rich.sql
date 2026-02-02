

$filtered_tests = (
    SELECT DISTINCT branch,
        suite_folder || '/' || test_name AS full_name
    FROM 
        `test_results/test_runs_column` AS t2
    WHERE 
        t2.build_type = 'relwithdebinfo'
        AND t2.status != 'skipped'
        AND t2.job_name != 'Run-tests'
        AND t2.run_timestamp > CurrentUtcDate() - 15 * Interval("P1D")
    GROUP BY 
        branch, suite_folder, test_name
    HAVING 
        COUNT(DISTINCT CASE 
            WHEN t2.job_name = 'PR-check'
            AND t2.status = 'failure'
            AND t2.pull IS NOT NULL
            AND t2.pull != ''
            AND String::Contains(t2.pull, 'PR_')
            THEN ListHead(
                Unicode::SplitToList(
                    CASE 
                        WHEN String::Contains(
                            ListHead(ListSkip(Unicode::SplitToList(CAST(t2.pull AS UTF8), 'PR_'), 1)),
                            '#'
                        ) THEN ListHead(
                            ListSkip(
                                Unicode::SplitToList(
                                    ListHead(ListSkip(Unicode::SplitToList(CAST(t2.pull AS UTF8), 'PR_'), 1)),
                                    '#'
                                ),
                                1
                            )
                        )
                        ELSE ListHead(ListSkip(Unicode::SplitToList(CAST(t2.pull AS UTF8), 'PR_'), 1))
                    END,
                    '_'
                )
            )
            ELSE NULL
        END) > 0
        AND COUNT(DISTINCT CASE 
            WHEN t2.job_name IN (
                'Nightly-run',
                'Regression-run',
                'Regression-run_Large',
                'Regression-run_Small_and_Medium',
                'Regression-run_compatibility',
                'Regression-whitelist-run',
                'Postcommit_relwithdebinfo', 
                'Postcommit_asan'
            )
            AND t2.status = 'passed'
            THEN t2.job_id 
            ELSE NULL
        END) > 0
        AND COUNT(DISTINCT CASE 
            WHEN t2.job_name IN (
                'Nightly-run',
                'Regression-run',
                'Regression-run_Large',
                'Regression-run_Small_and_Medium',
                'Regression-run_compatibility',
                'Regression-whitelist-run',
                'Postcommit_relwithdebinfo', 
                'Postcommit_asan'
            )
            AND t2.status != 'passed'
            AND t2.status != 'skipped'
            AND t2.status != 'mute'
            THEN t2.job_id 
            ELSE NULL
        END) = 0
        AND COUNT(DISTINCT CASE 
            WHEN t2.job_name IN (
                'Nightly-run',
                'Regression-run',
                'Regression-run_Large',
                'Regression-run_Small_and_Medium',
                'Regression-run_compatibility',
                'Regression-whitelist-run',
                'Postcommit_relwithdebinfo', 
                'Postcommit_asan'
            )
            AND t2.status = 'mute'
            THEN t2.job_id 
            ELSE NULL
        END) = 0
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
            WHEN NOT String::Contains(base.pull, '_A') THEN 1
            ELSE COALESCE(
                CAST(
                    ListHead(
                        Unicode::SplitToList(
                            ListHead(
                                ListSkip(
                                    Unicode::SplitToList(CAST(base.pull AS UTF8), '_A'),
                                    1
                                )
                            ),
                            '_'
                        )
                    ) AS Int32
                ),
                1
            )
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
        AND base.run_timestamp > CurrentUtcDate() - 15 * Interval("P1D")
        AND base.pull IS NOT NULL
        AND base.pull != ''
        AND String::Contains(base.pull, 'PR_')
        AND base.job_id IS NOT NULL
        AND base.branch IS NOT NULL
        AND base.suite_folder IS NOT NULL
        AND base.test_name IS NOT NULL
);

$all_failures_with_pr = (
    SELECT 
        full_name,
        suite_folder,
        test_name,
        job_id,
        run_timestamp,
        branch,
        status_description,
        pr_number,
        attempt_number,
        CASE WHEN job_id = MAX_BY(job_id, run_timestamp) OVER (PARTITION BY pr_number) THEN 1 ELSE 0 END AS is_last_run_in_pr
    FROM 
        $all_failures_with_pr_base
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