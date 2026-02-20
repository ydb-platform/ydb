-- Daily bug counts per (date_window, owner_team). One row per day per team.
-- Bug = issue with max_branch != '-' (assigned to a release branch). Uses test_results/analytics/github_issues_timeline.
-- Owner_team from area_to_owner_mapping at read time (same logic as github_issues_timeline_with_owner_from_mapping: prefix match by area).
-- SLA counts: low >= 30d, med/high/noprio >= 7d (out of SLA).
--
$sla_low_days = 30;
$sla_med_days = 7;
$sla_high_days = 7;
$sla_noprio_days = 7;

-- Owner by area (prefix match), same as timeline view
$owner_mapping = (
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
);

$bugs = (
    SELECT
        t.date AS date,
        t.project_item_id AS project_item_id,
        t.created_date AS created_date,
        t.priority AS priority,
        CASE
            WHEN t.area = 'area/-' THEN 'unknown'
            WHEN o.owner_team IS NOT NULL THEN o.owner_team
            ELSE 'team_unmatched:' || t.area
        END AS owner_team
    FROM `test_results/analytics/github_issues_timeline` AS t
    LEFT JOIN $owner_mapping AS o ON t.area = o.area
    WHERE t.date >= CurrentUtcDate() - 365 * Interval("P1D")
      AND t.is_open_at_end_of_day = 1
      AND t.max_branch IS NOT NULL AND t.max_branch != '-'
);

$with_days = (
    SELECT
        b.*,
        DateTime::ToDays(Cast(b.date AS Date) - Cast(b.created_date AS Date)) AS days_open
    FROM $bugs AS b
);

-- One row per (date_window, owner_team): daily snapshot.
-- total_* = count by priority (all bugs); *_out_of_sla = count exceeding SLA (low 30d, med/high/noprio 7d).
SELECT
    date AS date_window,
    Cast(owner_team AS Utf8) AS owner_team,
    COALESCE(COUNT(*), 0) AS total,
    COALESCE(COUNT(CASE WHEN priority LIKE '%low%' THEN 1 ELSE NULL END), 0) AS total_low,
    COALESCE(COUNT(CASE WHEN priority LIKE '%med%' THEN 1 ELSE NULL END), 0) AS total_med,
    COALESCE(COUNT(CASE WHEN priority LIKE '%high%' THEN 1 ELSE NULL END), 0) AS total_high,
    COALESCE(COUNT(CASE WHEN priority IS NULL OR (priority NOT LIKE '%low%' AND priority NOT LIKE '%med%' AND priority NOT LIKE '%high%') THEN 1 ELSE NULL END), 0) AS total_noprio,
    COALESCE(COUNT(CASE WHEN (priority LIKE '%low%') AND days_open >= $sla_low_days THEN 1 ELSE NULL END), 0) AS low_out_of_sla,
    COALESCE(COUNT(CASE WHEN (priority LIKE '%med%') AND days_open >= $sla_med_days THEN 1 ELSE NULL END), 0) AS med_out_of_sla,
    COALESCE(COUNT(CASE WHEN (priority LIKE '%high%') AND days_open >= $sla_high_days THEN 1 ELSE NULL END), 0) AS high_out_of_sla,
    COALESCE(COUNT(CASE WHEN (priority IS NULL OR (priority NOT LIKE '%low%' AND priority NOT LIKE '%med%' AND priority NOT LIKE '%high%')) AND days_open >= $sla_noprio_days THEN 1 ELSE NULL END), 0) AS noprio_out_of_sla
FROM $with_days
GROUP BY date, owner_team;
