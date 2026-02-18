-- Все PR с информацией об упавших тестах в PR-check.
--
-- Логика:
--   1. Берём все PR из github_data/pull_requests (дедуплицируем по последнему exported_at).
--   2. Для каждого PR ищем последний запуск PR-check за $test_lookback_days дней.
--   3. Определяем, заблокирован ли PR тестами:
--      - is_pr_blocked_by_tests_in_last_run_and_try = 1, если в последнем запуске
--        на третьей попытке (attempt = 3) есть падения.
--      - Третья попытка — финальная; если тест упал и там, значит PR заблокирован.
--   4. LEFT JOIN к детальной информации о каждом упавшем тесте:
--      full_name, job_id, branch, logs, owners и т.д.
--
-- Результат: одна строка на каждую пару (PR, упавший тест). PR без падений тоже включены
-- (с пустыми полями теста).
--
-- Параметры:
--   $test_lookback_days — окно выборки тестов (дни). Ограничивает сканирование таблицы
--     test_runs_column. PR с тестами старше этого периода не попадут в выборку.
--     По умолчанию 65 ≈ 2 месяца (запас для долгоживущих PR).

PRAGMA AnsiInForEmptyOrNullableItemsCollections;

$test_lookback_days = 65;

SELECT 
    pr.pr_number AS pr_number,
    pr.project_item_id AS pr_project_item_id,
    pr.pr_id AS pr_pr_id,
    pr.title AS pr_title,
    pr.url AS pr_url,
    pr.state AS pr_state,
    pr.body AS pr_body,
    pr.body_text AS pr_body_text,
    pr.is_draft AS pr_is_draft,
    pr.additions AS pr_additions,
    pr.deletions AS pr_deletions,
    pr.changed_files AS pr_changed_files,
    pr.mergeable AS pr_mergeable,
    pr.merged AS pr_merged,
    pr.created_at AS pr_created_at,
    pr.updated_at AS pr_updated_at,
    pr.closed_at AS pr_closed_at,
    pr.merged_at AS pr_merged_at,
    pr.created_date AS pr_created_date,
    pr.updated_date AS pr_updated_date,
    pr.days_since_created AS pr_days_since_created,
    pr.days_since_updated AS pr_days_since_updated,
    pr.time_to_close_hours AS pr_time_to_close_hours,
    pr.time_to_merge_hours AS pr_time_to_merge_hours,
    pr.author_login AS pr_author_login,
    pr.author_url AS pr_author_url,
    pr.repository_name AS pr_repository_name,
    pr.repository_url AS pr_repository_url,
    pr.head_ref_name AS pr_head_ref_name,
    pr.base_ref_name AS pr_base_ref_name,
    pr.assignees AS pr_assignees,
    pr.labels AS pr_labels,
    pr.milestone AS pr_milestone,
    pr.project_fields AS pr_project_fields,
    pr.review_decision AS pr_review_decision,
    pr.total_reviews_count AS pr_total_reviews_count,
    pr.total_comments_count AS pr_total_comments_count,
    pr.merged_by_login AS pr_merged_by_login,
    pr.merged_by_url AS pr_merged_by_url,
    pr.info AS pr_info,
    pr.exported_at AS pr_exported_at,
    CASE 
        WHEN pr.merged = 1 THEN 'merged'
        WHEN pr.state = 'OPEN' THEN 'open'
        WHEN pr.state = 'CLOSED' THEN 'closed'
        ELSE COALESCE(pr.state, 'unknown')
    END AS pr_status,
    COALESCE(block.is_pr_blocked_by_tests_in_last_run_and_try, 0) AS is_pr_blocked_by_tests_in_last_run_and_try,
    block.last_run_url AS last_run_url,
    COALESCE(t.full_name, CAST("" AS Utf8)) AS full_name,
    t.suite_folder AS suite_folder,
    t.test_name AS test_name,
    COALESCE(t.job_id, 0UL) AS job_id,
    t.run_url AS run_url,
    COALESCE(t.last_run_timestamp, Timestamp("1970-01-01T00:00:00.000000Z")) AS last_run_timestamp,
    t.branch AS branch,
    t.build_type AS build_type,
    t.status_description AS status_description,
    t.attempt_number AS attempt_number,
    t.is_last_run_in_pr AS is_last_run_in_pr,
    t.owners AS owners,
    t.log AS log,
    t.logsdir AS logsdir,
    t.stderr AS stderr,
    t.stdout AS stdout,
    t.error_type AS error_type,
    t.metadata AS metadata,
    t.metrics AS metrics
FROM 
    (
        SELECT 
            pr_number,
            project_item_id,
            pr_id,
            title,
            url,
            state,
            body,
            body_text,
            is_draft,
            additions,
            deletions,
            changed_files,
            mergeable,
            merged,
            created_at,
            updated_at,
            closed_at,
            merged_at,
            created_date,
            updated_date,
            days_since_created,
            days_since_updated,
            time_to_close_hours,
            time_to_merge_hours,
            author_login,
            author_url,
            repository_name,
            repository_url,
            head_ref_name,
            base_ref_name,
            assignees,
            labels,
            milestone,
            project_fields,
            review_decision,
            total_reviews_count,
            total_comments_count,
            merged_by_login,
            merged_by_url,
            info,
            exported_at
        FROM (
            SELECT 
                pr_number,
                project_item_id,
                pr_id,
                title,
                url,
                state,
                body,
                body_text,
                is_draft,
                additions,
                deletions,
                changed_files,
                mergeable,
                merged,
                created_at,
                updated_at,
                closed_at,
                merged_at,
                created_date,
                updated_date,
                days_since_created,
                days_since_updated,
                time_to_close_hours,
                time_to_merge_hours,
                author_login,
                author_url,
                repository_name,
                repository_url,
                head_ref_name,
                base_ref_name,
                assignees,
                labels,
                milestone,
                project_fields,
                review_decision,
                total_reviews_count,
                total_comments_count,
                merged_by_login,
                merged_by_url,
                info,
                exported_at,
                ROW_NUMBER() OVER (PARTITION BY pr_number ORDER BY exported_at DESC, created_date DESC) AS rn
            FROM 
                `github_data/pull_requests`
        ) AS ranked
        WHERE 
            ranked.rn = 1
    ) AS pr
LEFT JOIN 
    (
        SELECT 
            pr_number,
            CASE WHEN attempt_number = 3 AND has_failure = 1 THEN 1 ELSE 0 END AS is_pr_blocked_by_tests_in_last_run_and_try,
            CASE WHEN attempt_number = 3 AND has_failure = 1 THEN 'https://github.com/ydb-platform/ydb/actions/runs/' || CAST(job_id AS UTF8) ELSE NULL END AS last_run_url
        FROM (
            SELECT 
                pr_number,
                job_id,
                run_timestamp,
                attempt_number,
                has_failure,
                ROW_NUMBER() OVER (PARTITION BY pr_number ORDER BY run_timestamp DESC) AS rn
            FROM (
                SELECT 
                    r.job_id AS job_id,
                    r.pr_number AS pr_number,
                    MAX(r.run_timestamp) AS run_timestamp,
                    MAX_BY(r.attempt_number, r.run_timestamp) AS attempt_number,
                    MAX(CASE WHEN r.status = 'failure' AND r.attempt_number = la.last_attempt THEN 1 ELSE 0 END) AS has_failure
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
                        ) AS pr_number,
                        CASE 
                            WHEN String::Contains(pull, 'attempt_') THEN COALESCE(CAST(ListHead(Unicode::SplitToList(ListHead(ListSkip(Unicode::SplitToList(CAST(pull AS UTF8), 'attempt_'), 1)), '_')) AS Int32), 1)
                            WHEN String::Contains(pull, '_A') THEN COALESCE(CAST(ListHead(Unicode::SplitToList(ListHead(ListSkip(Unicode::SplitToList(CAST(pull AS UTF8), '_A'), 1)), '_')) AS Int32), 1)
                            ELSE 1
                        END AS attempt_number,
                        status
                    FROM 
                        `test_results/test_runs_column`
                    WHERE 
                        build_type = 'relwithdebinfo'
                        AND job_name = 'PR-check'
                        AND run_timestamp > CurrentUtcDate() - $test_lookback_days * Interval("P1D")
                        AND pull IS NOT NULL
                        AND pull != ''
                        AND String::Contains(pull, 'PR_')
                        AND job_id IS NOT NULL
                ) AS r
                INNER JOIN (
                    SELECT 
                        job_id,
                        pr_number,
                        MAX_BY(attempt_number, run_timestamp) AS last_attempt
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
                            AND run_timestamp > CurrentUtcDate() - $test_lookback_days * Interval("P1D")
                            AND pull IS NOT NULL
                            AND pull != ''
                            AND String::Contains(pull, 'PR_')
                            AND job_id IS NOT NULL
                    ) AS pr_check_runs
                    GROUP BY 
                        job_id,
                        pr_number
                ) AS la
                ON r.job_id = la.job_id AND r.pr_number = la.pr_number
                GROUP BY 
                    r.job_id,
                    r.pr_number
            ) AS job_agg
        ) AS last_run_per_pr
        WHERE 
            rn = 1
    ) AS block
    ON block.pr_number = CAST(pr.pr_number AS Utf8)
LEFT JOIN 
    (
        -- All failed tests from last job_id per PR (attempt 3)
        SELECT 
            tests.pr_number AS pr_number,
            tests.full_name AS full_name,
            tests.suite_folder AS suite_folder,
            tests.test_name AS test_name,
            tests.job_id AS job_id,
            'https://github.com/ydb-platform/ydb/actions/runs/' || CAST(tests.job_id AS UTF8) AS run_url,
            tests.run_timestamp AS last_run_timestamp,
            tests.branch AS branch,
            'relwithdebinfo' AS build_type,
            COALESCE(tests.status_description, '') AS status_description,
            tests.attempt_number AS attempt_number,
            1 AS is_last_run_in_pr,
            -- Prefer stable owners from testowners (derived from main), fallback to owners from test run row.
            COALESCE(o.owners, tests.owners) AS owners,
            tests.log AS log,
            tests.logsdir AS logsdir,
            tests.stderr AS stderr,
            tests.stdout AS stdout,
            tests.error_type AS error_type,
            tests.metadata AS metadata,
            tests.metrics AS metrics
        FROM (
            SELECT 
                job_id,
                run_timestamp,
                suite_folder,
                test_name,
                suite_folder || '/' || test_name AS full_name,
                branch,
                status_description,
                owners,
                log,
                logsdir,
                stderr,
                stdout,
                error_type,
                metadata,
                metrics,
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
                AND status = 'failure'
                AND run_timestamp > CurrentUtcDate() - $test_lookback_days * Interval("P1D")
                AND pull IS NOT NULL
                AND pull != ''
                AND String::Contains(pull, 'PR_')
                AND job_id IS NOT NULL
                AND branch IS NOT NULL
                AND suite_folder IS NOT NULL
                AND test_name IS NOT NULL
        ) AS tests
        LEFT JOIN
            `test_results/analytics/testowners` AS o
            ON o.suite_folder = tests.suite_folder
            AND o.test_name = tests.test_name
        INNER JOIN (
            -- Last job_id for each PR (attempt 3)
            SELECT 
                pr_number,
                MAX_BY(job_id, run_timestamp) AS last_job_id
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
                    AND run_timestamp > CurrentUtcDate() - $test_lookback_days * Interval("P1D")
                    AND pull IS NOT NULL
                    AND pull != ''
                    AND String::Contains(pull, 'PR_')
                    AND job_id IS NOT NULL
            ) AS all_runs
            WHERE attempt_number = 3
            GROUP BY pr_number
        ) AS last_job
        ON tests.pr_number = last_job.pr_number 
        AND tests.job_id = last_job.last_job_id
        WHERE tests.attempt_number = 3
    ) AS t
    ON CAST(t.pr_number AS Uint64) = pr.pr_number
ORDER BY 
    pr.pr_number,
    t.last_run_timestamp DESC,
    t.full_name
