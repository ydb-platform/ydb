-- Pre-aggregation: muted tests by team and by day, per build_type.
-- Filters: is_test_chunk = 0, is_muted = 1 (as required for BI).
-- effective_area / effective_owner_team are computed in tests_monitor.py (single source of truth).
-- Per-test previous effective owner and change date: tests_monitor.previous_effective_owner_team,
-- effective_owner_team_changed_date (also in muted_tests_with_issue_and_area).
-- This mart only aggregates; grid still uses timeline + mapping for empty cells.
$window_days = 365;
$muted_sla_days = 30;

$normalize = ($raw_area) -> {
    $parts = String::SplitToList(Cast($raw_area AS String), '/');
    RETURN Cast(
        IF(ListLength($parts) >= 2, $parts[0] || '/' || $parts[1], Cast($raw_area AS String))
    AS Utf8);
};

$mapping = (
    SELECT $normalize(area) AS area, owner_team AS owner_team
    FROM `test_results/analytics/area_to_owner_mapping`
);

$area_list = (
    SELECT DISTINCT $normalize(area) AS area
    FROM `test_results/analytics/github_issues_timeline`
    WHERE area IS NOT NULL AND max_branch != '-'
    UNION
    SELECT DISTINCT area AS area FROM $mapping
    UNION
    SELECT Cast('area/-' AS Utf8) AS area
);

$area_prefix_owner = (
    SELECT area, owner_team FROM (
        SELECT
            a.area AS area,
            om.owner_team AS owner_team,
            ROW_NUMBER() OVER (PARTITION BY a.area ORDER BY LENGTH(om.area) DESC) AS rn
        FROM $area_list AS a
        CROSS JOIN $mapping AS om
        WHERE a.area = om.area OR StartsWith(a.area, om.area || Utf8('/'))
    )
    WHERE rn = 1
);

$owner_grid = (
    SELECT
        al.area AS area,
        Cast(CASE
            WHEN al.area = 'area/-' THEN 'unknown'
            WHEN p.owner_team IS NOT NULL THEN p.owner_team
            ELSE 'team_unmatched:' || al.area
        END AS Utf8) AS owner_team
    FROM $area_list AS al
    LEFT JOIN $area_prefix_owner AS p ON al.area = p.area
);

$tm = (
    SELECT
        t.*,
        Unicode::ToLower(Cast(Coalesce(String::ReplaceAll(t.owner, 'TEAM:@ydb-platform/', ''), '') AS Utf8)) AS owner_team_key
    FROM `test_results/analytics/tests_monitor` AS t
    WHERE t.date_window >= CurrentUtcDate() - $window_days * Interval("P1D")
      --AND t.build_type = 'relwithdebinfo'
      AND t.is_test_chunk = 0
      AND t.state != 'Skipped'
);

$om = (
    SELECT owner_team AS owner_team, MIN(area) AS area
    FROM $mapping
    GROUP BY owner_team
);

$base = (
    SELECT
        tm.date_window AS date_window,
        Coalesce(tm.effective_owner_team, tm.owner_team_key) AS owner_team,
        Coalesce(tm.effective_area, $normalize(Coalesce(om.area, 'area/-'))) AS area,
        tm.branch AS branch,
        tm.build_type AS build_type,
        tm.full_name AS full_name,
        tm.is_muted AS is_muted,
        tm.days_in_mute_state AS days_in_mute_state
    FROM $tm AS tm
    LEFT JOIN $om AS om ON tm.owner_team_key = om.owner_team
);

$agg = (
    SELECT
        base.date_window AS date_window,
        base.owner_team AS owner_team,
        base.area AS area,
        base.branch AS branch,
        base.build_type AS build_type,
        COUNT(DISTINCT CASE WHEN base.is_muted = 1 THEN base.full_name ELSE NULL END) AS muted_count,
        COUNT(
            DISTINCT CASE
                WHEN base.is_muted = 1 AND base.days_in_mute_state > $muted_sla_days
                THEN base.full_name
                ELSE NULL
            END
        ) AS muted_count_more_30_days
    FROM $base AS base
    GROUP BY
        base.date_window,
        base.owner_team,
        base.area,
        base.branch,
        base.build_type
);

$all_area_owner = (
    SELECT DISTINCT owner_team AS owner_team, area AS area FROM $base
    UNION
    SELECT DISTINCT owner_team AS owner_team, area AS area FROM $owner_grid
);

$dates = (
    SELECT DISTINCT t.date AS date_window
    FROM `test_results/analytics/github_issues_timeline` AS t
    WHERE t.date >= CurrentUtcDate() - $window_days * Interval("P1D")
    UNION
    SELECT CurrentUtcDate() AS date_window
);
$branches_builds = (SELECT DISTINCT branch AS branch, build_type AS build_type FROM $base);

$grid = (
    SELECT
        d.date_window AS date_window,
        ao.owner_team AS owner_team,
        ao.area AS area,
        bb.branch AS branch,
        bb.build_type AS build_type
    FROM $dates AS d
    CROSS JOIN $all_area_owner AS ao
    CROSS JOIN $branches_builds AS bb
);

SELECT
    g.date_window AS date_window,
    g.owner_team AS owner_team,
    g.area AS area,
    g.branch AS branch,
    g.build_type AS build_type,
    COALESCE(a.muted_count, 0) AS muted_count,
    COALESCE(a.muted_count_more_30_days, 0) AS muted_count_more_30_days
FROM $grid AS g
LEFT JOIN $agg AS a
    ON a.date_window = g.date_window
    AND a.owner_team = g.owner_team
    AND a.area = g.area
    AND a.branch = g.branch
    AND a.build_type = g.build_type;
