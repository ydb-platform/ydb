-- Daily bug counts per (date_window, owner_team, area). One row per day per team per area.
-- Bug = issue with max_branch != '-' (assigned to a release branch). Uses test_results/analytics/github_issues_timeline.
-- Owner_team from area_to_owner_mapping at read time (same logic as ля: prefix match by area).
-- SLA counts: low >= 30d, med/high/noprio >= 7d (out of SLA).
--
$sla_low_days = 30;
$sla_med_days = 7;
$sla_high_days = 7;
$sla_noprio_days = 7;
$window_days = 365;

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
        t.area AS area,
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
    WHERE t.date >= CurrentUtcDate() - $window_days * Interval("P1D")
      AND t.is_open_at_end_of_day = 1
      AND t.max_branch IS NOT NULL AND t.max_branch != '-'
);

$with_days = (
    SELECT
        b.*,
        DateTime::ToDays(Cast(b.date AS Date) - Cast(b.created_date AS Date)) AS days_open
    FROM $bugs AS b
);

-- Aggregated counts per (date, owner_team, area). Will be left-joined to grid so missing combinations become 0.
$agg = (
    SELECT
        date AS date_window,
        Cast(owner_team AS Utf8) AS owner_team,
        Cast(area AS Utf8) AS area,
        COUNT(*) AS total,
        COUNT(CASE WHEN priority LIKE '%low%' THEN 1 ELSE NULL END) AS total_low,
        COUNT(CASE WHEN priority NOT LIKE '%low%' OR priority IS NULL THEN 1 ELSE NULL END) AS total_not_low,
        COUNT(CASE WHEN priority LIKE '%med%' THEN 1 ELSE NULL END) AS total_med,
        COUNT(CASE WHEN priority LIKE '%high%' THEN 1 ELSE NULL END) AS total_high,
        COUNT(CASE WHEN priority IS NULL OR (priority NOT LIKE '%low%' AND priority NOT LIKE '%med%' AND priority NOT LIKE '%high%') THEN 1 ELSE NULL END) AS total_noprio,
        COUNT(CASE WHEN (priority LIKE '%low%') AND days_open >= $sla_low_days THEN 1 ELSE NULL END) AS low_out_of_sla,
        COUNT(CASE WHEN (priority LIKE '%med%') AND days_open >= $sla_med_days THEN 1 ELSE NULL END) AS med_out_of_sla,
        COUNT(CASE WHEN (priority LIKE '%high%') AND days_open >= $sla_high_days THEN 1 ELSE NULL END) AS high_out_of_sla,
        COUNT(CASE WHEN (priority IS NULL OR (priority NOT LIKE '%low%' AND priority NOT LIKE '%med%' AND priority NOT LIKE '%high%')) AND days_open >= $sla_noprio_days THEN 1 ELSE NULL END) AS noprio_out_of_sla
    FROM $with_days
    GROUP BY date, owner_team, area
);

-- Full grid: every (date, owner_team, area) that appears in the period. Missing combinations get 0. Always include today.
$dates = (SELECT DISTINCT date AS date FROM $with_days UNION SELECT CurrentUtcDate() AS date);
$owner_area = (SELECT DISTINCT Cast(owner_team AS Utf8) AS owner_team, Cast(area AS Utf8) AS area FROM $with_days);
$grid = (SELECT d.date AS date_window, o.owner_team AS owner_team, o.area AS area FROM $dates AS d CROSS JOIN $owner_area AS o);

-- One row per (date_window, owner_team, area): daily snapshot. Missing (day, owner, area) → 0.
-- total_* = count by priority (all bugs); *_out_of_sla = count exceeding SLA (low 30d, med/high/noprio 7d).
SELECT
    g.date_window AS date_window,
    g.owner_team AS owner_team,
    g.area AS area,
    COALESCE(a.total, 0) AS total,
    COALESCE(a.total_low, 0) AS total_low,
    COALESCE(a.total_not_low, 0) AS total_not_low,
    COALESCE(a.total_med, 0) AS total_med,
    COALESCE(a.total_high, 0) AS total_high,
    COALESCE(a.total_noprio, 0) AS total_noprio,
    COALESCE(a.low_out_of_sla, 0) AS low_out_of_sla,
    COALESCE(a.med_out_of_sla, 0) AS med_out_of_sla,
    COALESCE(a.high_out_of_sla, 0) AS high_out_of_sla,
    COALESCE(a.noprio_out_of_sla, 0) AS noprio_out_of_sla
FROM $grid AS g
LEFT JOIN $agg AS a ON g.date_window = a.date_window AND g.owner_team = a.owner_team AND g.area = a.area;
