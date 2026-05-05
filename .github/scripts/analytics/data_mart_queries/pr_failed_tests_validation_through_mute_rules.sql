-- All PR-check failures (latest run per PR) with no postcommit/regression filter + PR data and mute policy.
--
-- Unlike pr_blocked_by_failed_tests_rich: we do not filter out tests that failed in regression/postcommit.
-- Here the result set includes all PR-check failures from the latest run per PR — including flaky tests
-- and tests that already fail in postcommit. So mute rule checks (met_mute_criteria, raw, raw_custom)
-- give meaningful results: you can see which failures would have qualified for mute.
--
-- Logic:
--   1. All PR-check failures in the last $pr_check_lookback_days days with pr_number, job_id, etc.
--   2. For each PR take the latest PR-check run by time; keep only failures from that run.
--   3. Join PR from github_data, tests_monitor (today, run day), aggregates over $mute_days (monitor + raw).
--   4. mute_resolution_json, mute_resolution_match, mute_resolution_match_custom — same as in pr_blocked_by_failed_tests_rich_with_pr_and_mute.
--
-- Parameters: same as in pr_blocked_by_failed_tests_rich_with_pr_and_mute ($lookback_days, $mute_days, mute and custom thresholds).

PRAGMA AnsiInForEmptyOrNullableItemsCollections;

$pr_check_lookback_days = 20;
$lookback_days = 1;
$mute_days = 4;
$mute_high_runs_bound = 10;
$mute_fail_threshold_high = 3;
$mute_fail_threshold_low = 2;
$mute_custom_high_runs_bound = 5;
$mute_custom_fail_threshold_low = 1;
$mute_custom_fail_threshold_high = 2;

-- All PR-check failures with pr_number (no regression/postcommit filter)
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

-- Only failures from the latest run per PR (no regression filter)
$failures_in_last_pr_run = (
    SELECT
        full_name,
        suite_folder,
        test_name,
        pr_number,
        job_id,
        run_timestamp,
        branch,
        CAST('relwithdebinfo' AS Utf8) AS build_type,
        status_description,
        attempt_number
    FROM
        $all_failures_with_pr
    WHERE
        pr_number IS NOT NULL
        AND job_id IS NOT NULL
        AND is_last_run_in_pr = 1
        AND run_timestamp > CurrentUtcDate() - $lookback_days * Interval("P1D")
);

-- Distinct (full_name, branch, build_type, run_date) from the result set
$keys_mute_window = (
    SELECT DISTINCT
        full_name,
        branch,
        build_type,
        CAST(run_timestamp AS Date) AS run_date
    FROM
        $failures_in_last_pr_run
);

-- tests_monitor aggregate over $mute_days days up to and including the failure day
$monitor_mute_window = (
    SELECT
        k.full_name AS am_full_name,
        k.branch AS am_branch,
        k.build_type AS am_build_type,
        k.run_date AS am_run_date,
        SUM(COALESCE(m.pass_count, 0)) AS pass_count_window,
        SUM(COALESCE(m.fail_count, 0)) AS fail_count_window,
        MAX_BY(CAST(COALESCE(m.is_muted, 0) AS Uint8), m.date_window) AS is_muted_in_window
    FROM
        $keys_mute_window AS k
    INNER JOIN
        `test_results/analytics/tests_monitor` AS m
        ON m.full_name = k.full_name
        AND m.branch = k.branch
        AND m.build_type = k.build_type
    WHERE
        m.date_window >= k.run_date - ($mute_days - 1) * Interval("P1D")
        AND m.date_window <= k.run_date
    GROUP BY
        k.full_name,
        k.branch,
        k.build_type,
        k.run_date
);

$raw_runs_for_window = (
    SELECT
        branch,
        suite_folder || '/' || test_name AS full_name,
        build_type,
        CAST(run_timestamp AS Date) AS run_date,
        status
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
        AND status IN ('passed', 'failure')
        AND run_timestamp > CurrentUtcDate() - ($lookback_days + $mute_days) * Interval("P1D")
);

$raw_mute_window = (
    SELECT
        k.full_name AS raw_full_name,
        k.branch AS raw_branch,
        k.build_type AS raw_build_type,
        k.run_date AS raw_run_date,
        SUM(CASE WHEN r.status = 'passed' THEN 1 ELSE 0 END) AS pass_count_raw,
        SUM(CASE WHEN r.status = 'failure' THEN 1 ELSE 0 END) AS fail_count_raw
    FROM
        $keys_mute_window AS k
    INNER JOIN
        $raw_runs_for_window AS r
        ON r.full_name = k.full_name
        AND r.branch = k.branch
        AND r.build_type = k.build_type
    WHERE
        r.run_date >= k.run_date - ($mute_days - 1) * Interval("P1D")
        AND r.run_date <= k.run_date
    GROUP BY
        k.full_name,
        k.branch,
        k.build_type,
        k.run_date
);

-- Runs that are not regression/postcommit (PR-check, Run-tests, etc.) in the same window — counts only for context.
$non_regression_runs_for_window = (
    SELECT
        branch,
        suite_folder || '/' || test_name AS full_name,
        build_type,
        CAST(run_timestamp AS Date) AS run_date,
        status
    FROM
        `test_results/test_runs_column`
    WHERE
        build_type = 'relwithdebinfo'
        AND job_name NOT IN (
            'Nightly-run',
            'Regression-run',
            'Regression-run_Large',
            'Regression-run_Small_and_Medium',
            'Regression-run_compatibility',
            'Regression-whitelist-run',
            'Postcommit_relwithdebinfo',
            'Postcommit_asan'
        )
        AND status IN ('passed', 'failure')
        AND run_timestamp > CurrentUtcDate() - ($lookback_days + $mute_days) * Interval("P1D")
);

$non_regression_mute_window = (
    SELECT
        k.full_name AS nr_full_name,
        k.branch AS nr_branch,
        k.build_type AS nr_build_type,
        k.run_date AS nr_run_date,
        SUM(CASE WHEN r.status = 'passed' THEN 1 ELSE 0 END) AS pass_count_nr,
        SUM(CASE WHEN r.status = 'failure' THEN 1 ELSE 0 END) AS fail_count_nr
    FROM
        $keys_mute_window AS k
    INNER JOIN
        $non_regression_runs_for_window AS r
        ON r.full_name = k.full_name
        AND r.branch = k.branch
        AND r.build_type = k.build_type
    WHERE
        r.run_date >= k.run_date - ($mute_days - 1) * Interval("P1D")
        AND r.run_date <= k.run_date
    GROUP BY
        k.full_name,
        k.branch,
        k.build_type,
        k.run_date
);

SELECT
    t.full_name AS full_name,
    t.suite_folder AS suite_folder,
    t.test_name AS test_name,
    t.pr_number AS pr_number,
    t.job_id AS job_id,
    CAST('https://github.com/ydb-platform/ydb/actions/runs/' || CAST(t.job_id AS UTF8) AS String) AS run_url,
    t.run_timestamp AS last_run_timestamp,
    t.branch AS branch,
    CAST(t.build_type AS String) AS build_type,
    CAST(COALESCE(t.status_description, '') AS String) AS status_description,
    CAST(COALESCE(t.attempt_number, 1) AS Int32) AS attempt_number,
    1 AS is_last_run_in_pr,
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
    COALESCE(m.owner, '') AS owner_today,
    (
        '{"monitor":{'
        || '"pass_count":' || CAST(COALESCE(am.pass_count_window, 0) AS Utf8)
        || ',"fail_count":' || CAST(COALESCE(am.fail_count_window, 0) AS Utf8)
        || ',"total_runs":' || CAST(COALESCE(am.pass_count_window, 0) + COALESCE(am.fail_count_window, 0) AS Utf8)
        || ',"is_muted_in_window":' || CAST(CAST(COALESCE(am.is_muted_in_window, 0) AS Uint8) AS Utf8)
        || ',"required_fail_count":' || CASE WHEN am.am_run_date IS NULL THEN 'null' WHEN (COALESCE(am.pass_count_window, 0) + COALESCE(am.fail_count_window, 0)) > $mute_high_runs_bound THEN CAST($mute_fail_threshold_high AS Utf8) ELSE CAST($mute_fail_threshold_low AS Utf8) END
        || ',"met_mute_criteria":' || CASE WHEN am.am_run_date IS NULL THEN '0' WHEN COALESCE(am.is_muted_in_window, 0) = 1 THEN '0' WHEN (am.fail_count_window >= $mute_fail_threshold_high AND (am.pass_count_window + am.fail_count_window) > $mute_high_runs_bound) OR (am.fail_count_window >= $mute_fail_threshold_low AND (am.pass_count_window + am.fail_count_window) <= $mute_high_runs_bound) THEN '1' ELSE '0' END
        || ',"mute_criteria_reason":"' || CASE WHEN am.am_run_date IS NULL THEN 'no_data' WHEN COALESCE(am.is_muted_in_window, 0) = 1 THEN 'already_muted' WHEN (am.fail_count_window >= $mute_fail_threshold_high AND (am.pass_count_window + am.fail_count_window) > $mute_high_runs_bound) OR (am.fail_count_window >= $mute_fail_threshold_low AND (am.pass_count_window + am.fail_count_window) <= $mute_high_runs_bound) THEN 'would_mute' ELSE 'would_not_mute' END || '"'
        || ',"mute_criteria_reason_detailed":"' || String::ReplaceAll(CASE WHEN am.am_run_date IS NULL THEN 'no_data' WHEN COALESCE(am.is_muted_in_window, 0) = 1 THEN 'already_muted' WHEN (am.fail_count_window >= $mute_fail_threshold_high AND (am.pass_count_window + am.fail_count_window) > $mute_high_runs_bound) OR (am.fail_count_window >= $mute_fail_threshold_low AND (am.pass_count_window + am.fail_count_window) <= $mute_high_runs_bound) THEN 'would_mute: fail_count=' || CAST(am.fail_count_window AS Utf8) || ', total_runs=' || CAST(am.pass_count_window + am.fail_count_window AS Utf8) WHEN (am.pass_count_window + am.fail_count_window) > $mute_high_runs_bound THEN 'would_not_mute: need >=' || CAST($mute_fail_threshold_high AS Utf8) || ' failures for runs>' || CAST($mute_high_runs_bound AS Utf8) || ', had ' || CAST(am.fail_count_window AS Utf8) ELSE 'would_not_mute: need >=' || CAST($mute_fail_threshold_low AS Utf8) || ' failures for runs<=' || CAST($mute_high_runs_bound AS Utf8) || ', had ' || CAST(am.fail_count_window AS Utf8) END, '"', '\"')
        || '"}},"raw":{'
        || '"pass_count":' || CAST(COALESCE(raw.pass_count_raw, 0) AS Utf8)
        || ',"fail_count":' || CAST(COALESCE(raw.fail_count_raw, 0) AS Utf8)
        || ',"total_runs":' || CAST(COALESCE(raw.pass_count_raw, 0) + COALESCE(raw.fail_count_raw, 0) AS Utf8)
        || ',"required_fail_count":' || CASE WHEN raw.raw_run_date IS NULL THEN 'null' WHEN (COALESCE(raw.pass_count_raw, 0) + COALESCE(raw.fail_count_raw, 0)) > $mute_high_runs_bound THEN CAST($mute_fail_threshold_high AS Utf8) ELSE CAST($mute_fail_threshold_low AS Utf8) END
        || ',"met_mute_criteria":' || CASE WHEN raw.raw_run_date IS NULL THEN '0' WHEN (raw.fail_count_raw >= $mute_fail_threshold_high AND (raw.pass_count_raw + raw.fail_count_raw) > $mute_high_runs_bound) OR (raw.fail_count_raw >= $mute_fail_threshold_low AND (raw.pass_count_raw + raw.fail_count_raw) <= $mute_high_runs_bound) THEN '1' ELSE '0' END
        || ',"mute_criteria_reason":"' || CASE WHEN raw.raw_run_date IS NULL THEN 'no_data' WHEN (raw.fail_count_raw >= $mute_fail_threshold_high AND (raw.pass_count_raw + raw.fail_count_raw) > $mute_high_runs_bound) OR (raw.fail_count_raw >= $mute_fail_threshold_low AND (raw.pass_count_raw + raw.fail_count_raw) <= $mute_high_runs_bound) THEN 'would_mute' ELSE 'would_not_mute' END || '"'
        || ',"mute_criteria_reason_detailed":"' || String::ReplaceAll(CASE WHEN raw.raw_run_date IS NULL THEN 'no_data' WHEN (raw.fail_count_raw >= $mute_fail_threshold_high AND (raw.pass_count_raw + raw.fail_count_raw) > $mute_high_runs_bound) OR (raw.fail_count_raw >= $mute_fail_threshold_low AND (raw.pass_count_raw + raw.fail_count_raw) <= $mute_high_runs_bound) THEN 'would_mute: fail_count=' || CAST(raw.fail_count_raw AS Utf8) || ', total_runs=' || CAST(raw.pass_count_raw + raw.fail_count_raw AS Utf8) WHEN (raw.pass_count_raw + raw.fail_count_raw) > $mute_high_runs_bound THEN 'would_not_mute: need >=' || CAST($mute_fail_threshold_high AS Utf8) || ' failures for runs>' || CAST($mute_high_runs_bound AS Utf8) || ', had ' || CAST(raw.fail_count_raw AS Utf8) ELSE 'would_not_mute: need >=' || CAST($mute_fail_threshold_low AS Utf8) || ' failures for runs<=' || CAST($mute_high_runs_bound AS Utf8) || ', had ' || CAST(raw.fail_count_raw AS Utf8) END, '"', '\"')
        || '"}},"raw_custom":{'
        || '"pass_count":' || CAST(COALESCE(raw.pass_count_raw, 0) AS Utf8)
        || ',"fail_count":' || CAST(COALESCE(raw.fail_count_raw, 0) AS Utf8)
        || ',"total_runs":' || CAST(COALESCE(raw.pass_count_raw, 0) + COALESCE(raw.fail_count_raw, 0) AS Utf8)
        || ',"required_fail_count":' || CASE WHEN raw.raw_run_date IS NULL THEN 'null' WHEN (COALESCE(raw.pass_count_raw, 0) + COALESCE(raw.fail_count_raw, 0)) > $mute_custom_high_runs_bound THEN CAST($mute_custom_fail_threshold_high AS Utf8) ELSE CAST($mute_custom_fail_threshold_low AS Utf8) END
        || ',"met_mute_criteria":' || CASE WHEN raw.raw_run_date IS NULL THEN '0' WHEN (raw.fail_count_raw >= $mute_custom_fail_threshold_high AND (raw.pass_count_raw + raw.fail_count_raw) > $mute_custom_high_runs_bound) OR (raw.fail_count_raw >= $mute_custom_fail_threshold_low AND (raw.pass_count_raw + raw.fail_count_raw) <= $mute_custom_high_runs_bound) THEN '1' ELSE '0' END
        || ',"mute_criteria_reason":"' || CASE WHEN raw.raw_run_date IS NULL THEN 'no_data' WHEN (raw.fail_count_raw >= $mute_custom_fail_threshold_high AND (raw.pass_count_raw + raw.fail_count_raw) > $mute_custom_high_runs_bound) OR (raw.fail_count_raw >= $mute_custom_fail_threshold_low AND (raw.pass_count_raw + raw.fail_count_raw) <= $mute_custom_high_runs_bound) THEN 'would_mute' ELSE 'would_not_mute' END || '"'
        || ',"mute_criteria_reason_detailed":"' || String::ReplaceAll(CASE WHEN raw.raw_run_date IS NULL THEN 'no_data' WHEN (raw.fail_count_raw >= $mute_custom_fail_threshold_high AND (raw.pass_count_raw + raw.fail_count_raw) > $mute_custom_high_runs_bound) OR (raw.fail_count_raw >= $mute_custom_fail_threshold_low AND (raw.pass_count_raw + raw.fail_count_raw) <= $mute_custom_high_runs_bound) THEN 'would_mute: fail_count=' || CAST(raw.fail_count_raw AS Utf8) || ', total_runs=' || CAST(raw.pass_count_raw + raw.fail_count_raw AS Utf8) WHEN (raw.pass_count_raw + raw.fail_count_raw) > $mute_custom_high_runs_bound THEN 'would_not_mute: need >=' || CAST($mute_custom_fail_threshold_high AS Utf8) || ' failures for runs>' || CAST($mute_custom_high_runs_bound AS Utf8) || ', had ' || CAST(raw.fail_count_raw AS Utf8) ELSE 'would_not_mute: need >=' || CAST($mute_custom_fail_threshold_low AS Utf8) || ' failures for runs<=' || CAST($mute_custom_high_runs_bound AS Utf8) || ', had ' || CAST(raw.fail_count_raw AS Utf8) END, '"', '\"')
        || '"}},"non_regression":{'
        || '"pass_count":' || CAST(COALESCE(nr.pass_count_nr, 0) AS Utf8)
        || ',"fail_count":' || CAST(COALESCE(nr.fail_count_nr, 0) AS Utf8)
        || ',"total_runs":' || CAST(COALESCE(nr.pass_count_nr, 0) + COALESCE(nr.fail_count_nr, 0) AS Utf8)
        || '"}}'
    ) AS mute_resolution_json,
    CASE
        WHEN am.am_run_date IS NULL OR raw.raw_run_date IS NULL THEN NULL
        WHEN (CASE WHEN am.am_run_date IS NULL THEN 0 WHEN COALESCE(am.is_muted_in_window, 0) = 1 THEN 0 WHEN (am.fail_count_window >= $mute_fail_threshold_high AND (am.pass_count_window + am.fail_count_window) > $mute_high_runs_bound) OR (am.fail_count_window >= $mute_fail_threshold_low AND (am.pass_count_window + am.fail_count_window) <= $mute_high_runs_bound) THEN 1 ELSE 0 END) = (CASE WHEN raw.raw_run_date IS NULL THEN 0 WHEN (raw.fail_count_raw >= $mute_fail_threshold_high AND (raw.pass_count_raw + raw.fail_count_raw) > $mute_high_runs_bound) OR (raw.fail_count_raw >= $mute_fail_threshold_low AND (raw.pass_count_raw + raw.fail_count_raw) <= $mute_high_runs_bound) THEN 1 ELSE 0 END) THEN 1
        ELSE 0
    END AS mute_resolution_match,
    CASE
        WHEN am.am_run_date IS NULL THEN NULL
        WHEN (CASE WHEN am.am_run_date IS NULL THEN 0 WHEN COALESCE(am.is_muted_in_window, 0) = 1 THEN 0 WHEN (am.fail_count_window >= $mute_fail_threshold_high AND (am.pass_count_window + am.fail_count_window) > $mute_high_runs_bound) OR (am.fail_count_window >= $mute_fail_threshold_low AND (am.pass_count_window + am.fail_count_window) <= $mute_high_runs_bound) THEN 1 ELSE 0 END) = (CASE WHEN am.am_run_date IS NULL THEN 0 WHEN COALESCE(am.is_muted_in_window, 0) = 1 THEN 0 WHEN (am.fail_count_window >= $mute_custom_fail_threshold_high AND (am.pass_count_window + am.fail_count_window) > $mute_custom_high_runs_bound) OR (am.fail_count_window >= $mute_custom_fail_threshold_low AND (am.pass_count_window + am.fail_count_window) <= $mute_custom_high_runs_bound) THEN 1 ELSE 0 END) THEN 1
        ELSE 0
    END AS mute_resolution_match_custom
FROM
    $failures_in_last_pr_run AS t
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
    ON CAST(t.pr_number AS Uint64) = pr.pr_number
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
    ON t.full_name = m.m_full_name
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
    ON t.full_name = m_run.m_run_full_name
    AND t.branch = m_run.m_run_branch
    AND t.build_type = m_run.m_run_build_type
    AND m_run.date_window = CAST(t.run_timestamp AS Date)
LEFT JOIN
    $monitor_mute_window AS am
    ON t.full_name = am.am_full_name
    AND t.branch = am.am_branch
    AND t.build_type = am.am_build_type
    AND CAST(t.run_timestamp AS Date) = am.am_run_date
LEFT JOIN
    $raw_mute_window AS raw
    ON t.full_name = raw.raw_full_name
    AND t.branch = raw.raw_branch
    AND t.build_type = raw.raw_build_type
    AND CAST(t.run_timestamp AS Date) = raw.raw_run_date
LEFT JOIN
    $non_regression_mute_window AS nr
    ON t.full_name = nr.nr_full_name
    AND t.branch = nr.nr_branch
    AND t.build_type = nr.nr_build_type
    AND CAST(t.run_timestamp AS Date) = nr.nr_run_date
ORDER BY
    t.run_timestamp DESC,
    t.pr_number,
    t.full_name
