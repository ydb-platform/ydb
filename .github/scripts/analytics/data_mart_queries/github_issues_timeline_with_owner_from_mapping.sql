-- Query for DataLens dataset (with_owner etc). Timeline with owner_team from area_to_owner_mapping at read time (type 2: no materialized owner).
-- Use this in BI so that changing owner_area_mapping.json + sync is enough; no need to re-export timeline.
--
-- Variant: distinct areas from vitrina -> join with mapping (StartsWith prefix match) -> join back.
-- No CTE: inline subqueries. All JOINs use equality only; OR/StartsWith in WHERE of CROSS JOIN.

SELECT
    t.date AS date,
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
    t.labels_list AS labels_list,
    t.max_branch AS max_branch,
    t.env AS env,
    t.priority AS priority,
    t.branch AS branch,
    t.area AS area,
    CASE
        WHEN t.area = 'area/-' THEN 'unknown'
        WHEN o.owner_team IS NOT NULL THEN o.owner_team
        ELSE 'team_unmatched:' || t.area
    END AS owner_team,
    t.is_open_at_end_of_day AS is_open_at_end_of_day,
    t.closed_on_this_day AS closed_on_this_day,
    CAST(
        CASE
            WHEN t.priority LIKE '%low%' THEN DateTime::ToDays(Cast(t.date AS Date) - Cast(t.created_date AS Date)) < 30
            WHEN t.priority LIKE '%med%' OR t.priority LIKE '%high%' THEN DateTime::ToDays(Cast(t.date AS Date) - Cast(t.created_date AS Date)) < 7
            ELSE DateTime::ToDays(Cast(t.date AS Date) - Cast(t.created_date AS Date)) < 7
        END AS Uint8
    ) AS in_sla
FROM `test_results/analytics/github_issues_timeline` AS t
LEFT JOIN (
    SELECT area AS area, owner_team AS owner_team
    FROM (
        SELECT
            a.area AS area,
            om.owner_team AS owner_team,
            ROW_NUMBER() OVER (PARTITION BY a.area ORDER BY LENGTH(om.area) DESC) AS rn
        FROM (SELECT DISTINCT area AS area FROM `test_results/analytics/github_issues_timeline`) AS a
        CROSS JOIN `test_results/analytics/area_to_owner_mapping` AS om
        WHERE a.area = om.area OR StartsWith(a.area, om.area || '/')
    )
    WHERE rn = 1
) AS o ON t.area = o.area;
