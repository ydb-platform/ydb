-- Pre-aggregation: muted tests by team and by day, per build_type.
-- Filters: is_test_chunk = 0, is_muted = 1 (as required for BI).
-- One row per (date_window, owner_team, branch, build_type). area from area_to_owner_mapping (one area per owner_team). In BI: filter by build_type for per-config metrics, or sum for total.
SELECT
    base.date_window AS date_window,
    base.owner_team_key AS owner_team,
    base.area AS area,
    base.branch AS branch,
    base.build_type AS build_type,
    COUNT(DISTINCT base.full_name) AS muted_count,
    COUNT(DISTINCT CASE WHEN base.days_in_mute_state >= 30 THEN base.full_name ELSE NULL END) AS muted_count_more_30_days
FROM (
    SELECT
        tm.*,
        Coalesce(om.area, 'area/-') AS area
    FROM (
        SELECT
            t.*,
            Unicode::ToLower(Cast(Coalesce(String::ReplaceAll(t.owner, 'TEAM:@ydb-platform/', ''), '') AS Utf8)) AS owner_team_key
        FROM `test_results/analytics/tests_monitor` AS t
        WHERE t.date_window >= CurrentUtcDate() - 365 * Interval("P1D")
          AND t.build_type = 'relwithdebinfo'
          AND t.is_test_chunk = 0
          AND t.is_muted = 1
    ) AS tm
    LEFT JOIN (
        SELECT owner_team AS owner_team, MIN(area) AS area
        FROM `test_results/analytics/area_to_owner_mapping`
        GROUP BY owner_team
    ) AS om ON tm.owner_team_key = om.owner_team
) AS base
GROUP BY
    base.date_window,
    base.owner_team_key,
    base.area,
    base.branch,
    base.build_type;
