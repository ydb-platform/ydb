SELECT `q_1`.`e_9` AS `res_0`,
         `q_1`.`e_10` AS `res_1`,
         `q_1`.`e_11` AS `res_2`,
         `q_1`.`e_12` AS `res_3`,
         `q_0`.`e_4` || ': ' || 'p-' || CAST(`q_0`.`e_5` AS UTF8) || ',f-' || CAST(`q_0`.`e_6` AS UTF8) || IF(`q_0`.`e_7` > 0, ',m-' || CAST(`q_0`.`e_7` AS UTF8), '') || IF(`q_0`.`e_8` > 0, ',s-' || CAST(`q_0`.`e_8` AS UTF8), '') || ' :' || CAST(Math::Round(IF(`q_1`.`e_13` > 0, (CAST(`q_0`.`e_14` AS DOUBLE) / `q_1`.`e_13`) * 100, 0), -1) AS UTF8) || '%' AS `res_4`, `q_0`.`e_15` AS `res_5`
FROM 
    (SELECT sum(`q_2`.`e_20` + `q_2`.`e_21` + `q_2`.`e_22`) AS `e_13`,
         `e_9`,
         `e_10`,
         `e_11`,
         `e_12`
    FROM 
        (SELECT sum(`t1`.`pass_count`) AS `e_20`,
         sum(`t1`.`fail_count`) AS `e_21`,
         sum(`t1`.`mute_count`) AS `e_22`,
         `e_16`,
         `e_17`,
         `e_18`,
         `e_19`
        FROM 
            (SELECT hist.branch AS branch,
         hist.build_type AS build_type,
         hist.date_window AS date_window,
         hist.days_ago_window AS days_ago_window,
         hist.fail_count AS fail_count,
         hist.full_name AS full_name,
         hist.history AS history,
         hist.history_class AS history_class,
         hist.mute_count AS mute_count,
         owners_t.owners AS owners,
         hist.pass_count AS pass_count,
         COALESCE(owners_t.run_timestamp_last,
         NULL) AS run_timestamp_last,
         COALESCE(owners_t.is_muted,
         NULL) AS is_muted,
         hist.skip_count AS skip_count,
         hist.suite_folder AS suite_folder,
         hist.test_name AS test_name from
                (SELECT *
                FROM `test_results/analytics/flaky_tests_window_1_days`
                WHERE date_window >= CurrentUtcDate() - 90 * Interval("P1D") ) AS hist
                LEFT JOIN 
                    (SELECT test_name,
         suite_folder,
         owners,
         run_timestamp_last,
         is_muted,
         branch,
         date
                    FROM `test_results/all_tests_with_owner_and_mute`
                    WHERE date >= CurrentUtcDate() - 90 * Interval("P1D") ) AS owners_t
                        ON hist.test_name = owners_t.test_name
                            AND hist.suite_folder = owners_t.suite_folder
                            AND hist.date_window = owners_t.date
                            AND hist.branch = owners_t.branch ) AS `t1`
                    WHERE `t1`.`branch` = 'main'
                            AND `t1`.`date_window` >= CurrentUtcDate() - 7 * Interval("P1D")
                            AND IF(String::Contains(`t1`.`full_name`, 'chunk chunk')
                            OR String::Contains(`t1`.`full_name`, 'chunk+chunk'), True, False) IN (False)
                            AND `t1`.`build_type` IN ('relwithdebinfo')
                            AND String::Contains(Unicode::ToLower(CAST(`t1`.`full_name` AS UTF8)), 'ydb/core/tx/schemeshard/ut_pq_reboots/tpqgrouptestreboots.createdrop-pqconfigtransactionsatschemeshard-true')
                    GROUP BY  `t1`.`date_window` AS `e_16`, `t1`.`branch` AS `e_17`, `t1`.`build_type` AS `e_18`, `t1`.`full_name` AS `e_19`) AS `q_2`
                    GROUP BY  `q_2`.`e_16` AS `e_9`, `q_2`.`e_17` AS `e_10`, `q_2`.`e_18` AS `e_11`, `q_2`.`e_19` AS `e_12`) AS `q_1`
                JOIN 
                (SELECT max(CASE
                    WHEN (`t1`.`is_muted` = 1) THEN
                    CASE
                    WHEN (String::Contains(`t1`.`history_class`, 'mute')
                        OR String::Contains(`t1`.`history_class`, 'failure')) THEN
                    'Muted Flaky'
                    WHEN String::Contains(`t1`.`history_class`, 'pass') THEN
                    'Muted Stable'
                    WHEN (String::Contains(`t1`.`history_class`, 'skipped')
                        OR `t1`.`history_class` IS NULL
                        OR `t1`.`history_class` = '') THEN
                    'Skipped'
                    ELSE `t1`.`history_class`
                    END
                    WHEN (`t1`.`is_muted` = 0) THEN
                    CASE
                    WHEN String::Contains(`t1`.`history_class`, 'failure') THEN
                    'Flaky'
                    WHEN String::Contains(`t1`.`history_class`, 'pass') THEN
                    'Passed'
                    WHEN (String::Contains(`t1`.`history_class`, 'skipped')
                        OR `t1`.`history_class` IS NULL
                        OR `t1`.`history_class` = '') THEN
                    'Skipped'
                    ELSE `t1`.`history_class`
                    END ELSE
                    CASE
                    WHEN String::Contains(`t1`.`history_class`, 'failure') THEN
                    'Flaky'
                    WHEN String::Contains(`t1`.`history_class`, 'mute') THEN
                    'Muted'
                    WHEN String::Contains(`t1`.`history_class`, 'pass') THEN
                    'Passed'
                    WHEN (String::Contains(`t1`.`history_class`, 'skipped')
                        OR `t1`.`history_class` IS NULL
                        OR `t1`.`history_class` = '') THEN
                    'Skipped'
                    ELSE `t1`.`history_class`
                    END END) AS `e_4`, max(`t1`.`pass_count`) AS `e_5`, max(`t1`.`fail_count`) AS `e_6`, max(`t1`.`mute_count`) AS `e_7`, max(`t1`.`skip_count`) AS `e_8`, sum(`t1`.`pass_count`) AS `e_14`, min(CASE
                    WHEN (CASE
                    WHEN (`t1`.`is_muted` = 1) THEN
                    CASE
                    WHEN (String::Contains(`t1`.`history_class`, 'mute')
                        OR String::Contains(`t1`.`history_class`, 'failure')) THEN
                    'Muted Flaky'
                    WHEN String::Contains(`t1`.`history_class`, 'pass') THEN
                    'Muted Stable'
                    WHEN (String::Contains(`t1`.`history_class`, 'skipped')
                        OR `t1`.`history_class` IS NULL
                        OR `t1`.`history_class` = '') THEN
                    'Skipped'
                    ELSE `t1`.`history_class`
                    END
                    WHEN (`t1`.`is_muted` = 0) THEN
                    CASE
                    WHEN String::Contains(`t1`.`history_class`, 'failure') THEN
                    'Flaky'
                    WHEN String::Contains(`t1`.`history_class`, 'pass') THEN
                    'Passed'
                    WHEN (String::Contains(`t1`.`history_class`, 'skipped')
                        OR `t1`.`history_class` IS NULL
                        OR `t1`.`history_class` = '') THEN
                    'Skipped'
                    ELSE `t1`.`history_class`
                    END ELSE
                    CASE
                    WHEN String::Contains(`t1`.`history_class`, 'failure') THEN
                    'Flaky'
                    WHEN String::Contains(`t1`.`history_class`, 'mute') THEN
                    'Muted'
                    WHEN String::Contains(`t1`.`history_class`, 'pass') THEN
                    'Passed'
                    WHEN (String::Contains(`t1`.`history_class`, 'skipped')
                        OR `t1`.`history_class` IS NULL
                        OR `t1`.`history_class` = '') THEN
                    'Skipped'
                    ELSE `t1`.`history_class`
                    END END IN ('failure', 'Flaky')) THEN
                    -1
                    WHEN (CASE
                    WHEN (`t1`.`is_muted` = 1) THEN
                    CASE
                    WHEN (String::Contains(`t1`.`history_class`, 'mute')
                        OR String::Contains(`t1`.`history_class`, 'failure')) THEN
                    'Muted Flaky'
                    WHEN String::Contains(`t1`.`history_class`, 'pass') THEN
                    'Muted Stable'
                    WHEN (String::Contains(`t1`.`history_class`, 'skipped')
                        OR `t1`.`history_class` IS NULL
                        OR `t1`.`history_class` = '') THEN
                    'Skipped'
                    ELSE `t1`.`history_class`
                    END
                    WHEN (`t1`.`is_muted` = 0) THEN
                    CASE
                    WHEN String::Contains(`t1`.`history_class`, 'failure') THEN
                    'Flaky'
                    WHEN String::Contains(`t1`.`history_class`, 'pass') THEN
                    'Passed'
                    WHEN (String::Contains(`t1`.`history_class`, 'skipped')
                        OR `t1`.`history_class` IS NULL
                        OR `t1`.`history_class` = '') THEN
                    'Skipped'
                    ELSE `t1`.`history_class`
                    END ELSE
                    CASE
                    WHEN String::Contains(`t1`.`history_class`, 'failure') THEN
                    'Flaky'
                    WHEN String::Contains(`t1`.`history_class`, 'mute') THEN
                    'Muted'
                    WHEN String::Contains(`t1`.`history_class`, 'pass') THEN
                    'Passed'
                    WHEN (String::Contains(`t1`.`history_class`, 'skipped')
                        OR `t1`.`history_class` IS NULL
                        OR `t1`.`history_class` = '') THEN
                    'Skipped'
                    ELSE `t1`.`history_class`
                    END END IN ('Muted Flaky')) THEN
                    -1
                    WHEN (CASE
                    WHEN (`t1`.`is_muted` = 1) THEN
                    CASE
                    WHEN (String::Contains(`t1`.`history_class`, 'mute')
                        OR String::Contains(`t1`.`history_class`, 'failure')) THEN
                    'Muted Flaky'
                    WHEN String::Contains(`t1`.`history_class`, 'pass') THEN
                    'Muted Stable'
                    WHEN (String::Contains(`t1`.`history_class`, 'skipped')
                        OR `t1`.`history_class` IS NULL
                        OR `t1`.`history_class` = '') THEN
                    'Skipped'
                    ELSE `t1`.`history_class`
                    END
                    WHEN (`t1`.`is_muted` = 0) THEN
                    CASE
                    WHEN String::Contains(`t1`.`history_class`, 'failure') THEN
                    'Flaky'
                    WHEN String::Contains(`t1`.`history_class`, 'pass') THEN
                    'Passed'
                    WHEN (String::Contains(`t1`.`history_class`, 'skipped')
                        OR `t1`.`history_class` IS NULL
                        OR `t1`.`history_class` = '') THEN
                    'Skipped'
                    ELSE `t1`.`history_class`
                    END ELSE
                    CASE
                    WHEN String::Contains(`t1`.`history_class`, 'failure') THEN
                    'Flaky'
                    WHEN String::Contains(`t1`.`history_class`, 'mute') THEN
                    'Muted'
                    WHEN String::Contains(`t1`.`history_class`, 'pass') THEN
                    'Passed'
                    WHEN (String::Contains(`t1`.`history_class`, 'skipped')
                        OR `t1`.`history_class` IS NULL
                        OR `t1`.`history_class` = '') THEN
                    'Skipped'
                    ELSE `t1`.`history_class`
                    END END IN ('Muted Stable')) THEN
                    1
                    WHEN (CASE
                    WHEN (`t1`.`is_muted` = 1) THEN
                    CASE
                    WHEN (String::Contains(`t1`.`history_class`, 'mute')
                        OR String::Contains(`t1`.`history_class`, 'failure')) THEN
                    'Muted Flaky'
                    WHEN String::Contains(`t1`.`history_class`, 'pass') THEN
                    'Muted Stable'
                    WHEN (String::Contains(`t1`.`history_class`, 'skipped')
                        OR `t1`.`history_class` IS NULL
                        OR `t1`.`history_class` = '') THEN
                    'Skipped'
                    ELSE `t1`.`history_class`
                    END
                    WHEN (`t1`.`is_muted` = 0) THEN
                    CASE
                    WHEN String::Contains(`t1`.`history_class`, 'failure') THEN
                    'Flaky'
                    WHEN String::Contains(`t1`.`history_class`, 'pass') THEN
                    'Passed'
                    WHEN (String::Contains(`t1`.`history_class`, 'skipped')
                        OR `t1`.`history_class` IS NULL
                        OR `t1`.`history_class` = '') THEN
                    'Skipped'
                    ELSE `t1`.`history_class`
                    END ELSE
                    CASE
                    WHEN String::Contains(`t1`.`history_class`, 'failure') THEN
                    'Flaky'
                    WHEN String::Contains(`t1`.`history_class`, 'mute') THEN
                    'Muted'
                    WHEN String::Contains(`t1`.`history_class`, 'pass') THEN
                    'Passed'
                    WHEN (String::Contains(`t1`.`history_class`, 'skipped')
                        OR `t1`.`history_class` IS NULL
                        OR `t1`.`history_class` = '') THEN
                    'Skipped'
                    ELSE `t1`.`history_class`
                    END END IN ('Passed')) THEN
                    2
                    WHEN (CASE
                    WHEN (`t1`.`is_muted` = 1) THEN
                    CASE
                    WHEN (String::Contains(`t1`.`history_class`, 'mute')
                        OR String::Contains(`t1`.`history_class`, 'failure')) THEN
                    'Muted Flaky'
                    WHEN String::Contains(`t1`.`history_class`, 'pass') THEN
                    'Muted Stable'
                    WHEN (String::Contains(`t1`.`history_class`, 'skipped')
                        OR `t1`.`history_class` IS NULL
                        OR `t1`.`history_class` = '') THEN
                    'Skipped'
                    ELSE `t1`.`history_class`
                    END
                    WHEN (`t1`.`is_muted` = 0) THEN
                    CASE
                    WHEN String::Contains(`t1`.`history_class`, 'failure') THEN
                    'Flaky'
                    WHEN String::Contains(`t1`.`history_class`, 'pass') THEN
                    'Passed'
                    WHEN (String::Contains(`t1`.`history_class`, 'skipped')
                        OR `t1`.`history_class` IS NULL
                        OR `t1`.`history_class` = '') THEN
                    'Skipped'
                    ELSE `t1`.`history_class`
                    END ELSE
                    CASE
                    WHEN String::Contains(`t1`.`history_class`, 'failure') THEN
                    'Flaky'
                    WHEN String::Contains(`t1`.`history_class`, 'mute') THEN
                    'Muted'
                    WHEN String::Contains(`t1`.`history_class`, 'pass') THEN
                    'Passed'
                    WHEN (String::Contains(`t1`.`history_class`, 'skipped')
                        OR `t1`.`history_class` IS NULL
                        OR `t1`.`history_class` = '') THEN
                    'Skipped'
                    ELSE `t1`.`history_class`
                    END END IN ('Skipped')) THEN
                    0
                    ELSE 1 END) AS `e_15`, `t1`.`date_window` AS `e_16`, `t1`.`branch` AS `e_17`, `t1`.`build_type` AS `e_18`, `t1`.`full_name` AS `e_19`
                FROM 
                    (SELECT hist.branch AS branch,
         hist.build_type AS build_type,
         hist.date_window AS date_window,
         hist.days_ago_window AS days_ago_window,
         hist.fail_count AS fail_count,
         hist.full_name AS full_name,
         hist.history AS history,
         hist.history_class AS history_class,
         hist.mute_count AS mute_count,
         owners_t.owners AS owners,
         hist.pass_count AS pass_count,
         COALESCE(owners_t.run_timestamp_last,
         NULL) AS run_timestamp_last,
         COALESCE(owners_t.is_muted,
         NULL) AS is_muted,
         hist.skip_count AS skip_count,
         hist.suite_folder AS suite_folder,
         hist.test_name AS test_name from
                        (SELECT *
                        FROM `test_results/analytics/flaky_tests_window_1_days`
                        WHERE date_window >= CurrentUtcDate() - 90 * Interval("P1D") ) AS hist
                        LEFT JOIN 
                            (SELECT test_name,
         suite_folder,
         owners,
         run_timestamp_last,
         is_muted,
         branch,
         date
                            FROM `test_results/all_tests_with_owner_and_mute`
                            WHERE date >= CurrentUtcDate() - 90 * Interval("P1D") ) AS owners_t
                                ON hist.test_name = owners_t.test_name
                                    AND hist.suite_folder = owners_t.suite_folder
                                    AND hist.date_window = owners_t.date
                                    AND hist.branch = owners_t.branch ) AS `t1`
                            WHERE `t1`.`branch` = 'main'
                                    AND `t1`.`date_window` >= CurrentUtcDate() - 7 * Interval("P1D")
                                    AND IF(String::Contains(`t1`.`full_name`, 'chunk chunk')
                                    OR String::Contains(`t1`.`full_name`, 'chunk+chunk'), True, False) IN (False)
                                    AND `t1`.`build_type` IN ('relwithdebinfo')
                                    AND String::Contains(Unicode::ToLower(CAST(`t1`.`full_name` AS UTF8)), 'ydb/core/tx/schemeshard/ut_pq_reboots/tpqgrouptestreboots.createdrop-pqconfigtransactionsatschemeshard-true')
                            GROUP BY  `t1`.`date_window`, `t1`.`branch`, `t1`.`build_type`, `t1`.`full_name`) AS `q_0`
                            ON `q_1`.`e_9` = `q_0`.`e_16`
                            AND `q_1`.`e_10` = `q_0`.`e_17`
                        AND `q_1`.`e_11` = `q_0`.`e_18`
                    AND `q_1`.`e_12` = `q_0`.`e_19`;