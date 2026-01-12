SELECT `res_0`, PERCENTILE(`t1`.`days_in_state`, 0.9) AS `res_1`, count(`t1`.`test_name`) AS `res_2` 
FROM (
select * FROM `test_results/analytics/test_muted_monitor_mart`
where is_muted_or_skipped=1
) AS `t1` 
WHERE `t1`.`is_test_chunk` = 0 AND `t1`.`is_muted_or_skipped` = 1 AND `t1`.`date_window` = CurrentUtcDate() AND `t1`.`build_type` IN ('relwithdebinfo') AND `t1`.`branch` IN ('main') AND `t1`.`resolution` IN ('MUTED: delete candidate', 'MUTED: in sla', 'MUTED: delete candidate', 'MUTED: in sla') AND `t1`.`state` IN ('Muted Flaky', 'Muted Stable', 'Skipped', 'no_runs') GROUP BY `t1`.`owner_team` AS `res_0` ORDER BY `res_2` DESC
 LIMIT 1000001
