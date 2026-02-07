SELECT `res_0`,
         `res_1`,
         `res_2`,
         `res_3`,
         `res_4`,
         `res_5`,
         `res_6`,
         max(`t1`.`days_in_mute_state`) AS `res_7`,
         sum(CASE
    WHEN (`t1`.`state` = 'Muted Flaky') THEN
    1
    WHEN (`t1`.`state` = 'Muted Stable') THEN
    3
    ELSE 2 END) AS `res_8`, `res_9`, `res_10`
FROM 
    (SELECT *
    FROM `test_results/analytics/test_muted_monitor_mart`
    WHERE is_muted_or_skipped=1 ) AS `t1`
WHERE `t1`.`is_test_chunk` = 0
        AND `t1`.`date_window` = CurrentUtcDate()
        AND `t1`.`is_muted_or_skipped` = 1
        AND `t1`.`build_type` IN ('relwithdebinfo')
        AND `t1`.`branch` IN ('main')
        AND `t1`.`resolution` IN ('MUTED: delete candidate', 'MUTED: in sla', 'MUTED: delete candidate', 'MUTED: in sla')
        AND `t1`.`state` IN ('Muted Flaky', 'Muted Stable', 'Skipped', 'no_runs')
GROUP BY  `t1`.`owner_team` AS `res_0`, '(a ' || '"' || Unicode::ReplaceAll(CAST('https://github.com/ydb-platform/ydb/blob/' || `t1`.`branch` || '/' || `t1`.`suite_folder` AS UTF8), coalesce(CAST('"' AS UTF8), ''), coalesce(CAST('""' AS UTF8), '')) || '"' || ' ' || '"' || Unicode::ReplaceAll(CAST(`t1`.`suite_folder` AS UTF8), coalesce(CAST('"' AS UTF8), ''), coalesce(CAST('""' AS UTF8), '')) || '"' || ')' AS `res_1`, `t1`.`test_name` AS `res_2`, CAST(`t1`.`mute_state_change_date` AS UTF8) || ': ' || CAST(`t1`.`days_in_mute_state` AS UTF8) || ' д. назад' AS `res_3`,
    CASE
    WHEN (`t1`.`state` = 'Muted Flaky') THEN
    'Моргает последние ' || CAST(`t1`.`days_in_state` AS UTF8) || ' д.'
    WHEN (`t1`.`state` = 'Muted Stable') THEN
    'Стабилен последние ' || CAST(`t1`.`days_in_state` AS UTF8) || ' д.'
    ELSE 'Не выполнялся последние ' || CAST(`t1`.`days_in_state` AS UTF8) || ' д.'
    END AS `res_4`, '(a ' || '"' || Unicode::ReplaceAll(CAST('https://datalens.yandex/34xnbsom67hcq?&branch=' || `t1`.`branch` || '&full_name=' || `t1`.`full_name` AS UTF8), coalesce(CAST('"' AS UTF8), ''), coalesce(CAST('""' AS UTF8), '')) || '"' || ' "History")' AS `res_5`, '(c ' || '(c ' || '(a ' || '"' || Unicode::ReplaceAll(CAST(`t1`.`github_issue_url` AS UTF8), coalesce(CAST('"' AS UTF8), ''), coalesce(CAST('""' AS UTF8), '')) || '"' || ' ' || '"' || Unicode::ReplaceAll(CAST(CAST(`t1`.`github_issue_number` AS UTF8) || '-' || `t1`.`github_issue_state` AS UTF8), coalesce(CAST('"' AS UTF8), ''), coalesce(CAST('""' AS UTF8), '')) || '"' || ')' || ' "-")' || ' ' || '"' || Unicode::ReplaceAll(CAST(CAST(CAST(`t1`.`github_issue_created_at` AS DATE) AS UTF8) AS UTF8), coalesce(CAST('"' AS UTF8), ''), coalesce(CAST('""' AS UTF8), '')) || '"' || ')' AS `res_6`, `t1`.`days_in_mute_state` AS `res_9`, `t1`.`days_in_state` AS `res_10`
ORDER BY  `res_9` ASC,
         `res_10` ASC,
         `res_0` ASC,
         `res_1` ASC,
         `res_2` ASC,
         `res_3` ASC,
         `res_4` ASC,
         `res_5` ASC,
         `res_6` ASC LIMIT 1200 OFFSET 0
