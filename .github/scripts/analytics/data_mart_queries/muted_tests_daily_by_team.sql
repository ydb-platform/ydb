-- Pre-aggregation: muted tests by team and by day, per build_type.
-- Filters: is_test_chunk = 0, is_muted = 1 (as required for BI).
-- One row per (date_window, owner_team, branch, build_type). area from area_to_owner_mapping (one area per owner_team). In BI: filter by build_type for per-config metrics, or sum for total.
$window_days = 365;
$muted_sla_days = 30;

$tm = (
    SELECT
        t.*,
        Unicode::ToLower(Cast(Coalesce(String::ReplaceAll(t.owner, 'TEAM:@ydb-platform/', ''), '') AS Utf8)) AS owner_team_key
    FROM `test_results/analytics/tests_monitor` AS t
    WHERE t.date_window >= CurrentUtcDate() - $window_days * Interval("P1D")
      AND t.build_type = 'relwithdebinfo'
      AND t.is_test_chunk = 0
);

$base = (
    SELECT
        tm.date_window AS date_window,
        tm.owner_team_key AS owner_team,
        Coalesce(om.area, 'area/-') AS area,
        tm.branch AS branch,
        tm.build_type AS build_type,
        tm.full_name AS full_name,
        tm.is_muted AS is_muted,
        tm.days_in_mute_state AS days_in_mute_state
    FROM $tm AS tm
    LEFT JOIN (
        SELECT owner_team AS owner_team, MIN(area) AS area
        FROM `test_results/analytics/area_to_owner_mapping`
        GROUP BY owner_team
    ) AS om ON tm.owner_team_key = om.owner_team
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
                WHEN base.is_muted = 1 AND base.days_in_mute_state >= $muted_sla_days
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

$dates = (SELECT DISTINCT date_window AS date_window FROM $tm);
$dims = (
    SELECT DISTINCT
        owner_team AS owner_team,
        area AS area,
        branch AS branch,
        build_type AS build_type
    FROM $base
);
$grid = (
    SELECT
        d.date_window AS date_window,
        x.owner_team AS owner_team,
        x.area AS area,
        x.branch AS branch,
        x.build_type AS build_type
    FROM $dates AS d
    CROSS JOIN $dims AS x
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
