-- For full reload via data_mart_executor_by_month.py. Issues on a daily timeline: for each date, which issues are open at end of day and which were closed that day.
-- Run: python3 .github/scripts/analytics/data_mart_executor_by_month.py --query_path .github/scripts/analytics/data_mart_queries/datalens_ds_queries/github_issues_timeline_full.sql --table_path test_results/analytics/github_issues_timeline --store_type column --partition_keys date --primary_keys date issue_number project_item_id
-- Optional: --by_month 12 (default)
-- FULL WINDOW: use with data_mart_executor (full 365-day window) or data_mart_executor_by_month (per-month).
-- Dates come from tests_monitor (date_window), no ListFromRange/FLATTEN BY.
-- In BI: filter by date, owner_team; for issue list per day — filter by date; for counts — GROUP BY date, SUM(is_open_at_end_of_day), SUM(closed_on_this_day).
--
-- Windows (change here or override via script):
--   $timeline_days   — date dimension and "open in window": include issues open on at least one day in [now - timeline_days, now].
--   Include: created_date <= today and (still open or closed within window: closed_at >= now - timeline_days).
$timeline_days = 365;
-- For by_month wrapper: script overwrites these to restrict to one month (avoids connection timeouts)
$month_start = Date("1970-01-01");
$month_end = Date("2100-01-01");

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
    i.branch AS branch,
    i.area AS area,
    CAST(
        (i.closed_at IS NULL OR Cast(i.closed_at AS Date) > dt.d) AS Uint8
    ) AS is_open_at_end_of_day,
    CAST(
        (i.closed_at IS NOT NULL AND Cast(i.closed_at AS Date) = dt.d) AS Uint8
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
        COALESCE(JSON_VALUE(t.info, "$.branch"), '-') AS branch,
        COALESCE(JSON_VALUE(t.info, "$.area"), 'area/-') AS area
    FROM `github_data/issues` AS t
    LEFT JOIN `test_results/analytics/area_to_owner_mapping` AS m
        ON m.area = COALESCE(JSON_VALUE(t.info, "$.area"), 'area/-')
    WHERE t.created_date <= CurrentUtcDate()
      AND (t.closed_at IS NULL OR Cast(t.closed_at AS Date) >= CurrentUtcDate() - $timeline_days * Interval("P1D"))
) AS i
WHERE i.created_date <= dt.d
  AND dt.d >= $month_start AND dt.d < $month_end;
