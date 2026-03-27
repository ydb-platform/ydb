-- Daily bug counts per (date_window, owner_team, area). One row per day per team per area.
-- Bug = issue with max_branch != '-' (assigned to a release branch).
-- Area normalized to first two path segments (area/cs/analytics → area/cs).
-- Owner_team via prefix match from area_to_owner_mapping (longest wins).
-- Grid: date spine from timeline in the window (+ today) × (area, owner); zeros via LEFT JOIN.
-- SLA: low >= 30d, med/high/noprio >= 7d.
--
$sla_low_days = 30;
$sla_med_days = 7;
$sla_high_days = 7;
$sla_noprio_days = 7;
$window_days = 365;

$normalize = ($raw_area) -> {
    $parts = String::SplitToList(Cast($raw_area AS String), '/');
    RETURN Cast(
        IF(ListLength($parts) >= 2, $parts[0] || '/' || $parts[1], Cast($raw_area AS String))
    AS Utf8);
};

-- Single read of mapping table, normalized once and reused everywhere
$mapping = (
    SELECT $normalize(area) AS area, owner_team AS owner_team
    FROM `test_results/analytics/area_to_owner_mapping`
);

$bugs_raw = (
    SELECT
        t.date AS date,
        $normalize(t.area) AS area,
        t.project_item_id AS project_item_id,
        t.created_date AS created_date,
        t.priority AS priority
    FROM `test_results/analytics/github_issues_timeline` AS t
    WHERE t.date >= CurrentUtcDate() - $window_days * Interval("P1D")
      AND t.is_open_at_end_of_day = 1
      AND t.max_branch IS NOT NULL AND t.max_branch != '-'
);

$area_list = (
    SELECT DISTINCT area AS area FROM $bugs_raw
    UNION
    SELECT DISTINCT area AS area FROM $mapping
    UNION
    SELECT Cast('area/-' AS Utf8) AS area
);

$owner = (
    SELECT
        al.area AS area,
        Cast(CASE
            WHEN al.area = 'area/-' THEN 'unknown'
            WHEN o.owner_team IS NOT NULL THEN o.owner_team
            ELSE 'team_unmatched:' || al.area
        END AS Utf8) AS owner_team
    FROM $area_list AS al
    LEFT JOIN (
        SELECT area AS area, owner_team AS owner_team FROM (
            SELECT a.area AS area, om.owner_team AS owner_team,
                   ROW_NUMBER() OVER (PARTITION BY a.area ORDER BY LENGTH(om.area) DESC) AS rn
            FROM $area_list AS a
            CROSS JOIN $mapping AS om
            WHERE a.area = om.area OR StartsWith(a.area, om.area || '/')
        ) WHERE rn = 1
    ) AS o ON al.area = o.area
);

-- 4) Enrich bugs with owner, compute days_open, aggregate
$bugs = (
    SELECT
        b.date AS date,
        b.area AS area,
        b.project_item_id AS project_item_id,
        b.priority AS priority,
        o.owner_team AS owner_team,
        DateTime::ToDays(Cast(b.date AS Date) - Cast(b.created_date AS Date)) AS days_open
    FROM $bugs_raw AS b
    LEFT JOIN $owner AS o ON b.area = o.area
);

$agg = (
    SELECT
        date AS date_window,
        owner_team AS owner_team,
        area AS area,
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
    FROM $bugs
    GROUP BY date, owner_team, area
);

-- Grid: date spine = all days present in timeline for the window (not only days with bugs)
$dates = (
    SELECT DISTINCT t.date AS date
    FROM `test_results/analytics/github_issues_timeline` AS t
    WHERE t.date >= CurrentUtcDate() - $window_days * Interval("P1D")
    UNION
    SELECT CurrentUtcDate() AS date
);
$grid = (SELECT d.date AS date_window, o.owner_team AS owner_team, o.area AS area FROM $dates AS d CROSS JOIN $owner AS o);

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
