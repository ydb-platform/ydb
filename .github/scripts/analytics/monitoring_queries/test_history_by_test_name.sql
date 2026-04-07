SELECT `res_0`,
         `res_1`,
         `res_2`,
         `res_3`,
         `res_4`,
         `res_5`,
         `res_6`,
         `res_7`,
         `res_8`
FROM 
    (SELECT *from `test_results/analytics/test_history_fast` ) AS `t1`
    WHERE `t1`.`branch` IN ('stable-25-3', 'stable-25-3')
        AND `t1`.`full_name` IN ('ydb/core/cms/ut_sentinel_unstable/unittest.sole chunk')
        AND `t1`.`build_type` IN ('relwithdebinfo', 'relwithdebinfo')
        AND `t1`.`status` IN ('mute', 'failure', 'passed', 'skipped')
        AND `t1`.`run_timestamp` >= CurrentUtcDatetime() - 7 * Interval("P1D")
GROUP BY  '(a ' || '"' || Unicode::ReplaceAll(CAST('https://github.com/ydb-platform/ydb/actions/runs/' || CAST(`t1`.`job_id` AS UTF8) AS UTF8),
coalesce(CAST('"' AS UTF8), ''),
coalesce(CAST('""' AS UTF8), '')) || '"' || ' ' || '"' || Unicode::ReplaceAll(CAST(ListHead(ListSkip(Unicode::SplitToList(CAST(`t1`.`pull` AS UTF8), '_A'), 1 - 1)) AS UTF8),
coalesce(CAST('"' AS UTF8), ''),
coalesce(CAST('""' AS UTF8), '')) || '"' || ')' AS `res_0`, `t1`.`run_timestamp` AS `res_1`, `t1`.`job_name` AS `res_2`, `t1`.`build_type` AS `res_3`, `t1`.`branch` AS `res_4`, `t1`.`status` AS `res_5`, `t1`.`duration` AS `res_6`, `t1`.`full_name` AS `res_7`, '(tooltip ' || '"' || Unicode::ReplaceAll(CAST(Unicode::Substring(CAST(`t1`.`status_description` AS UTF8), 0, 100) AS UTF8),
coalesce(CAST('"' AS UTF8), ''),
coalesce(CAST('""' AS UTF8), '')) || '"' || ' ' || '"' || Unicode::ReplaceAll(CAST(Unicode::ReplaceAll(CAST(`t1`.`status_description` AS UTF8),
coalesce(CAST(';;' AS UTF8), ''),
coalesce(CAST(' ' AS UTF8), '')) AS UTF8),
coalesce(CAST('"' AS UTF8), ''),
coalesce(CAST('""' AS UTF8), '')) || '"' || ' "top")' AS `res_8`
ORDER BY  `res_1` DESC,
         `res_0` ASC,
         `res_2` ASC,
         `res_3` ASC,
         `res_4` ASC,
         `res_5` ASC,
         `res_6` ASC,
         `res_7` ASC,
         `res_8` ASC LIMIT 200 OFFSET 0
