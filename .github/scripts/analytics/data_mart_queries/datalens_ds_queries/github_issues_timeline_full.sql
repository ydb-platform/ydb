-- For full reload via data_mart_executor.py. Issues on a daily timeline: for each date, which issues are open at end of day and which were closed that day.
-- Open/closed state and SLA start come from info.open_periods (close/reopen intervals from GitHub export; literal JSON paths by index).
-- Run: python3 .github/scripts/analytics/data_mart_executor.py --query_path .github/scripts/analytics/data_mart_queries/datalens_ds_queries/github_issues_timeline_full.sql --table_path test_results/analytics/github_issues_timeline --store_type column --partition_keys date --primary_keys date issue_number project_item_id --cleanup_window_key date --cleanup_window_interval '365 * Interval("P1D")'
-- FULL WINDOW: 365-day reload; $month_start/$month_end can be narrowed manually if the query times out.
-- Dates come from tests_monitor (date_window), no ListFromRange/FLATTEN BY for the date spine.
-- In BI: filter by date, owner_team; for issue list per day — filter by date; for counts — GROUP BY date, SUM(is_open_at_end_of_day), SUM(closed_on_this_day).
--
-- Windows (change here or override via script):
--   $timeline_days   — date dimension and "open in window": include issues with any open period overlapping [now - timeline_days, now].
$timeline_days = 365;
$max_open_periods = 50;
-- For by_month wrapper: script overwrites these to restrict to one month (avoids connection timeouts)
$month_start = Date("1970-01-01");
$month_end = Date("2100-01-01");

$json_period_start = ($info, $idx) -> {
    RETURN CASE $idx
        WHEN 0 THEN JSON_VALUE($info, '$.open_periods[0].start')
        WHEN 1 THEN JSON_VALUE($info, '$.open_periods[1].start')
        WHEN 2 THEN JSON_VALUE($info, '$.open_periods[2].start')
        WHEN 3 THEN JSON_VALUE($info, '$.open_periods[3].start')
        WHEN 4 THEN JSON_VALUE($info, '$.open_periods[4].start')
        WHEN 5 THEN JSON_VALUE($info, '$.open_periods[5].start')
        WHEN 6 THEN JSON_VALUE($info, '$.open_periods[6].start')
        WHEN 7 THEN JSON_VALUE($info, '$.open_periods[7].start')
        WHEN 8 THEN JSON_VALUE($info, '$.open_periods[8].start')
        WHEN 9 THEN JSON_VALUE($info, '$.open_periods[9].start')
        WHEN 10 THEN JSON_VALUE($info, '$.open_periods[10].start')
        WHEN 11 THEN JSON_VALUE($info, '$.open_periods[11].start')
        WHEN 12 THEN JSON_VALUE($info, '$.open_periods[12].start')
        WHEN 13 THEN JSON_VALUE($info, '$.open_periods[13].start')
        WHEN 14 THEN JSON_VALUE($info, '$.open_periods[14].start')
        WHEN 15 THEN JSON_VALUE($info, '$.open_periods[15].start')
        WHEN 16 THEN JSON_VALUE($info, '$.open_periods[16].start')
        WHEN 17 THEN JSON_VALUE($info, '$.open_periods[17].start')
        WHEN 18 THEN JSON_VALUE($info, '$.open_periods[18].start')
        WHEN 19 THEN JSON_VALUE($info, '$.open_periods[19].start')
        WHEN 20 THEN JSON_VALUE($info, '$.open_periods[20].start')
        WHEN 21 THEN JSON_VALUE($info, '$.open_periods[21].start')
        WHEN 22 THEN JSON_VALUE($info, '$.open_periods[22].start')
        WHEN 23 THEN JSON_VALUE($info, '$.open_periods[23].start')
        WHEN 24 THEN JSON_VALUE($info, '$.open_periods[24].start')
        WHEN 25 THEN JSON_VALUE($info, '$.open_periods[25].start')
        WHEN 26 THEN JSON_VALUE($info, '$.open_periods[26].start')
        WHEN 27 THEN JSON_VALUE($info, '$.open_periods[27].start')
        WHEN 28 THEN JSON_VALUE($info, '$.open_periods[28].start')
        WHEN 29 THEN JSON_VALUE($info, '$.open_periods[29].start')
        WHEN 30 THEN JSON_VALUE($info, '$.open_periods[30].start')
        WHEN 31 THEN JSON_VALUE($info, '$.open_periods[31].start')
        WHEN 32 THEN JSON_VALUE($info, '$.open_periods[32].start')
        WHEN 33 THEN JSON_VALUE($info, '$.open_periods[33].start')
        WHEN 34 THEN JSON_VALUE($info, '$.open_periods[34].start')
        WHEN 35 THEN JSON_VALUE($info, '$.open_periods[35].start')
        WHEN 36 THEN JSON_VALUE($info, '$.open_periods[36].start')
        WHEN 37 THEN JSON_VALUE($info, '$.open_periods[37].start')
        WHEN 38 THEN JSON_VALUE($info, '$.open_periods[38].start')
        WHEN 39 THEN JSON_VALUE($info, '$.open_periods[39].start')
        WHEN 40 THEN JSON_VALUE($info, '$.open_periods[40].start')
        WHEN 41 THEN JSON_VALUE($info, '$.open_periods[41].start')
        WHEN 42 THEN JSON_VALUE($info, '$.open_periods[42].start')
        WHEN 43 THEN JSON_VALUE($info, '$.open_periods[43].start')
        WHEN 44 THEN JSON_VALUE($info, '$.open_periods[44].start')
        WHEN 45 THEN JSON_VALUE($info, '$.open_periods[45].start')
        WHEN 46 THEN JSON_VALUE($info, '$.open_periods[46].start')
        WHEN 47 THEN JSON_VALUE($info, '$.open_periods[47].start')
        WHEN 48 THEN JSON_VALUE($info, '$.open_periods[48].start')
        WHEN 49 THEN JSON_VALUE($info, '$.open_periods[49].start')
        ELSE NULL
    END
};

$json_period_end = ($info, $idx) -> {
    RETURN CASE $idx
        WHEN 0 THEN JSON_VALUE($info, '$.open_periods[0].end')
        WHEN 1 THEN JSON_VALUE($info, '$.open_periods[1].end')
        WHEN 2 THEN JSON_VALUE($info, '$.open_periods[2].end')
        WHEN 3 THEN JSON_VALUE($info, '$.open_periods[3].end')
        WHEN 4 THEN JSON_VALUE($info, '$.open_periods[4].end')
        WHEN 5 THEN JSON_VALUE($info, '$.open_periods[5].end')
        WHEN 6 THEN JSON_VALUE($info, '$.open_periods[6].end')
        WHEN 7 THEN JSON_VALUE($info, '$.open_periods[7].end')
        WHEN 8 THEN JSON_VALUE($info, '$.open_periods[8].end')
        WHEN 9 THEN JSON_VALUE($info, '$.open_periods[9].end')
        WHEN 10 THEN JSON_VALUE($info, '$.open_periods[10].end')
        WHEN 11 THEN JSON_VALUE($info, '$.open_periods[11].end')
        WHEN 12 THEN JSON_VALUE($info, '$.open_periods[12].end')
        WHEN 13 THEN JSON_VALUE($info, '$.open_periods[13].end')
        WHEN 14 THEN JSON_VALUE($info, '$.open_periods[14].end')
        WHEN 15 THEN JSON_VALUE($info, '$.open_periods[15].end')
        WHEN 16 THEN JSON_VALUE($info, '$.open_periods[16].end')
        WHEN 17 THEN JSON_VALUE($info, '$.open_periods[17].end')
        WHEN 18 THEN JSON_VALUE($info, '$.open_periods[18].end')
        WHEN 19 THEN JSON_VALUE($info, '$.open_periods[19].end')
        WHEN 20 THEN JSON_VALUE($info, '$.open_periods[20].end')
        WHEN 21 THEN JSON_VALUE($info, '$.open_periods[21].end')
        WHEN 22 THEN JSON_VALUE($info, '$.open_periods[22].end')
        WHEN 23 THEN JSON_VALUE($info, '$.open_periods[23].end')
        WHEN 24 THEN JSON_VALUE($info, '$.open_periods[24].end')
        WHEN 25 THEN JSON_VALUE($info, '$.open_periods[25].end')
        WHEN 26 THEN JSON_VALUE($info, '$.open_periods[26].end')
        WHEN 27 THEN JSON_VALUE($info, '$.open_periods[27].end')
        WHEN 28 THEN JSON_VALUE($info, '$.open_periods[28].end')
        WHEN 29 THEN JSON_VALUE($info, '$.open_periods[29].end')
        WHEN 30 THEN JSON_VALUE($info, '$.open_periods[30].end')
        WHEN 31 THEN JSON_VALUE($info, '$.open_periods[31].end')
        WHEN 32 THEN JSON_VALUE($info, '$.open_periods[32].end')
        WHEN 33 THEN JSON_VALUE($info, '$.open_periods[33].end')
        WHEN 34 THEN JSON_VALUE($info, '$.open_periods[34].end')
        WHEN 35 THEN JSON_VALUE($info, '$.open_periods[35].end')
        WHEN 36 THEN JSON_VALUE($info, '$.open_periods[36].end')
        WHEN 37 THEN JSON_VALUE($info, '$.open_periods[37].end')
        WHEN 38 THEN JSON_VALUE($info, '$.open_periods[38].end')
        WHEN 39 THEN JSON_VALUE($info, '$.open_periods[39].end')
        WHEN 40 THEN JSON_VALUE($info, '$.open_periods[40].end')
        WHEN 41 THEN JSON_VALUE($info, '$.open_periods[41].end')
        WHEN 42 THEN JSON_VALUE($info, '$.open_periods[42].end')
        WHEN 43 THEN JSON_VALUE($info, '$.open_periods[43].end')
        WHEN 44 THEN JSON_VALUE($info, '$.open_periods[44].end')
        WHEN 45 THEN JSON_VALUE($info, '$.open_periods[45].end')
        WHEN 46 THEN JSON_VALUE($info, '$.open_periods[46].end')
        WHEN 47 THEN JSON_VALUE($info, '$.open_periods[47].end')
        WHEN 48 THEN JSON_VALUE($info, '$.open_periods[48].end')
        WHEN 49 THEN JSON_VALUE($info, '$.open_periods[49].end')
        ELSE NULL
    END
};

$period_indices = (
    SELECT idx AS idx
    FROM (SELECT ListFromRange(0, $max_open_periods) AS idxs) AS src
    FLATTEN LIST BY idxs AS idx
);

$issue_periods = (
    SELECT
        t.project_item_id AS project_item_id,
        t.issue_number AS issue_number,
        Cast($json_period_start(t.info, pi.idx) AS Date) AS period_start,
        Cast($json_period_end(t.info, pi.idx) AS Date) AS period_end
    FROM `github_data/issues` AS t
    CROSS JOIN $period_indices AS pi
    WHERE JSON_VALUE(t.info, '$.open_periods[0].start') IS NOT NULL
      AND $json_period_start(t.info, pi.idx) IS NOT NULL
    UNION ALL
    SELECT
        t.project_item_id AS project_item_id,
        t.issue_number AS issue_number,
        t.created_date AS period_start,
        Cast(t.closed_at AS Date) AS period_end
    FROM `github_data/issues` AS t
    WHERE JSON_VALUE(t.info, '$.open_periods[0].start') IS NULL
);

$issues_in_window = (
    SELECT DISTINCT
        ip.project_item_id AS project_item_id,
        ip.issue_number AS issue_number
    FROM $issue_periods AS ip
    WHERE ip.period_start <= CurrentUtcDate()
      AND (ip.period_end IS NULL OR ip.period_end >= CurrentUtcDate() - $timeline_days * Interval("P1D"))
);

SELECT
    dt.d AS date,
    i.project_item_id AS project_item_id,
    i.issue_id AS issue_id,
    i.issue_number AS issue_number,
    i.title AS title,
    i.url AS url,
    i.state AS state,
    i.state_reason AS state_reason,
    i.created_at AS created_at,
    i.updated_at AS updated_at,
    i.closed_at AS closed_at,
    i.created_date AS created_date,
    i.updated_date AS updated_date,
    i.author_login AS author_login,
    i.author_url AS author_url,
    i.repository_name AS repository_name,
    i.repository_url AS repository_url,
    i.project_status AS project_status,
    i.project_owner AS project_owner,
    i.project_priority AS project_priority,
    i.is_in_project AS is_in_project,
    i.days_since_created AS days_since_created,
    i.days_since_updated AS days_since_updated,
    i.time_to_close_hours AS time_to_close_hours,
    i.assignees AS assignees,
    i.labels AS labels,
    i.milestone AS milestone,
    i.project_fields AS project_fields,
    i.info AS info,
    i.issue_type AS issue_type,
    i.exported_at AS exported_at,
    i.owner_team AS owner_team,
    i.labels_list AS labels_list,
    i.max_branch AS max_branch,
    i.env AS env,
    i.priority AS priority,
    i.releaseblocker_state AS releaseblocker_state,
    i.branch AS branch,
    i.area AS area,
    p.period_start AS sla_start_date,
    CAST(
        (p.period_start IS NOT NULL) AS Uint8
    ) AS is_open_at_end_of_day,
    CAST(
        (c.period_end IS NOT NULL) AS Uint8
    ) AS closed_on_this_day
FROM (
    SELECT DISTINCT date_window AS d
    FROM `test_results/analytics/tests_monitor`
    WHERE date_window >= CurrentUtcDate() - $timeline_days * Interval("P1D")
) AS dt
CROSS JOIN (
    SELECT
        t.project_item_id AS project_item_id,
        t.issue_id AS issue_id,
        t.issue_number AS issue_number,
        t.title AS title,
        t.url AS url,
        t.state AS state,
        t.state_reason AS state_reason,
        t.created_at AS created_at,
        t.updated_at AS updated_at,
        t.closed_at AS closed_at,
        t.created_date AS created_date,
        t.updated_date AS updated_date,
        t.author_login AS author_login,
        t.author_url AS author_url,
        t.repository_name AS repository_name,
        t.repository_url AS repository_url,
        t.project_status AS project_status,
        t.project_owner AS project_owner,
        t.project_priority AS project_priority,
        t.is_in_project AS is_in_project,
        t.days_since_created AS days_since_created,
        t.days_since_updated AS days_since_updated,
        t.time_to_close_hours AS time_to_close_hours,
        t.assignees AS assignees,
        t.labels AS labels,
        t.milestone AS milestone,
        t.project_fields AS project_fields,
        t.info AS info,
        t.issue_type AS issue_type,
        t.exported_at AS exported_at,
        COALESCE(m.owner_team, 'unknown') AS owner_team,
        CAST(JSON_QUERY(t.labels, "$.name" WITH UNCONDITIONAL ARRAY WRAPPER) AS String) AS labels_list,
        COALESCE(JSON_VALUE(t.info, "$.max_branch"), '-') AS max_branch,
        COALESCE(JSON_VALUE(t.info, "$.env"), 'env:-') AS env,
        COALESCE(JSON_VALUE(t.info, "$.priority"), 'priority:-') AS priority,
        COALESCE(JSON_VALUE(t.info, "$.releaseblocker_state"), 'release:-') AS releaseblocker_state,
        COALESCE(JSON_VALUE(t.info, "$.branch"), '-') AS branch,
        COALESCE(JSON_VALUE(t.info, "$.area"), 'area/-') AS area
    FROM `github_data/issues` AS t
    INNER JOIN $issues_in_window AS w
        ON w.project_item_id = t.project_item_id AND w.issue_number = t.issue_number
    LEFT JOIN `test_results/analytics/area_to_owner_mapping` AS m
        ON m.area = COALESCE(JSON_VALUE(t.info, "$.area"), 'area/-')
    WHERE t.created_date <= CurrentUtcDate()
) AS i
LEFT JOIN $issue_periods AS p
    ON p.project_item_id = i.project_item_id
    AND p.issue_number = i.issue_number
    AND p.period_start <= dt.d
    AND (p.period_end IS NULL OR p.period_end > dt.d)
LEFT JOIN $issue_periods AS c
    ON c.project_item_id = i.project_item_id
    AND c.issue_number = i.issue_number
    AND c.period_end = dt.d
WHERE i.created_date <= dt.d
  AND dt.d >= $month_start AND dt.d < $month_end;
