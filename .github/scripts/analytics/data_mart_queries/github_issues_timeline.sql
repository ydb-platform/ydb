-- Issues on a daily timeline: for each date, which issues are open at end of day and which were closed that day.
-- Open/closed state and SLA start from github_data/issue_open_periods (exported with issues).
-- RECENT DAYS: updates only the last $recent_days days (31 by default). Use with data_mart_executor for quick refresh.
-- Dates come from tests_monitor (date_window), no ListFromRange/FLATTEN BY for the date spine.
-- In BI: filter by date, owner_team; for issue list per day — filter by date; for counts — GROUP BY date, SUM(is_open_at_end_of_day), SUM(closed_on_this_day).
--
$timeline_days = 365;
$recent_days = 31;  -- only these days are selected (today and $recent_days-1 days back)

-- Owner by area (prefix match): area/cs/analytics -> area/cs in mapping. Return matched_area (om.area) for output.
$owner_mapping = (
    SELECT area AS area, owner_team AS owner_team, matched_area AS matched_area
    FROM (
        SELECT
            a.area AS area,
            om.owner_team AS owner_team,
            om.area AS matched_area,
            ROW_NUMBER() OVER (PARTITION BY a.area ORDER BY LENGTH(om.area) DESC) AS rn
        FROM (SELECT DISTINCT COALESCE(JSON_VALUE(info, "$.area"), 'area/-') AS area FROM `github_data/issues`) AS a
        CROSS JOIN `test_results/analytics/area_to_owner_mapping` AS om
        WHERE a.area = om.area OR StartsWith(a.area, om.area || '/')
    )
    WHERE rn = 1
);

$issue_periods = (
    SELECT
        p.project_item_id AS project_item_id,
        p.issue_number AS issue_number,
        p.period_start AS period_start,
        p.period_end AS period_end
    FROM `github_data/issue_open_periods` AS p
    UNION ALL
    SELECT
        t2.project_item_id AS project_item_id,
        t2.issue_number AS issue_number,
        t2.created_date AS period_start,
        Cast(t2.closed_at AS Date) AS period_end
    FROM `github_data/issues` AS t2
    LEFT JOIN (
        SELECT DISTINCT
            ep.project_item_id AS project_item_id,
            ep.issue_number AS issue_number
        FROM `github_data/issue_open_periods` AS ep
    ) AS has_periods
        ON has_periods.issue_number = t2.issue_number
        AND has_periods.project_item_id = t2.project_item_id
    WHERE has_periods.issue_number IS NULL
);

$issues_in_window = (
    SELECT DISTINCT
        ip.project_item_id AS project_item_id,
        ip.issue_number AS issue_number
    FROM $issue_periods AS ip
    WHERE ip.period_start <= CurrentUtcDate()
      AND (ip.period_end IS NULL OR ip.period_end >= CurrentUtcDate() - $timeline_days * Interval("P1D"))
);

$date_spine = (
    SELECT DISTINCT date_window AS d
    FROM `test_results/analytics/tests_monitor`
    WHERE date_window >= CurrentUtcDate() - $timeline_days * Interval("P1D")
);

$open_on_day = (
    SELECT
        dt.d AS date,
        ip.project_item_id AS project_item_id,
        ip.issue_number AS issue_number,
        ip.period_start AS sla_start_date
    FROM $date_spine AS dt
    CROSS JOIN $issue_periods AS ip
    WHERE ip.period_start <= dt.d
      AND (ip.period_end IS NULL OR ip.period_end > dt.d)
);

$closed_on_day = (
    SELECT DISTINCT
        ip.period_end AS date,
        ip.project_item_id AS project_item_id,
        ip.issue_number AS issue_number
    FROM $issue_periods AS ip
    WHERE ip.period_end IS NOT NULL
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
    p.sla_start_date AS sla_start_date,
    CAST(
        (p.sla_start_date IS NOT NULL) AS Uint8
    ) AS is_open_at_end_of_day,
    CAST(
        (c.issue_number IS NOT NULL) AS Uint8
    ) AS closed_on_this_day
FROM $date_spine AS dt
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
        Coalesce(m.matched_area,
            CASE
                WHEN ListLength(String::SplitToList(Cast(COALESCE(JSON_VALUE(t.info, "$.area"), 'area/-') AS String), '/')) >= 2
                THEN String::SplitToList(Cast(COALESCE(JSON_VALUE(t.info, "$.area"), 'area/-') AS String), '/')[0] || '/' || String::SplitToList(Cast(COALESCE(JSON_VALUE(t.info, "$.area"), 'area/-') AS String), '/')[1]
                ELSE COALESCE(JSON_VALUE(t.info, "$.area"), 'area/-')
            END
        ) AS area
    FROM `github_data/issues` AS t
    INNER JOIN $issues_in_window AS w
        ON w.project_item_id = t.project_item_id AND w.issue_number = t.issue_number
    LEFT JOIN $owner_mapping AS m ON m.area = COALESCE(JSON_VALUE(t.info, "$.area"), 'area/-')
    WHERE t.created_date <= CurrentUtcDate()
) AS i
LEFT JOIN $open_on_day AS p
    ON p.date = dt.d
    AND p.project_item_id = i.project_item_id
    AND p.issue_number = i.issue_number
LEFT JOIN $closed_on_day AS c
    ON c.date = dt.d
    AND c.project_item_id = i.project_item_id
    AND c.issue_number = i.issue_number
WHERE i.created_date <= dt.d
  AND dt.d >= CurrentUtcDate() - $recent_days * Interval("P1D");
