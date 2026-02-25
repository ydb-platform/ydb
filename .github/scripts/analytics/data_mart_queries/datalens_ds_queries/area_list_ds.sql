-- All distinct areas in format area/name (no subpaths: area/cs, not area/cs/analytics).
-- Sources: github_issues_timeline.area and area_to_owner_mapping.area; take first two path segments via SplitToList.
SELECT DISTINCT Cast(
    CASE
        WHEN ListLength(String::SplitToList(Cast(r.area AS String), '/')) >= 2
        THEN String::SplitToList(Cast(r.area AS String), '/')[0] || '/' || String::SplitToList(Cast(r.area AS String), '/')[1]
        ELSE Cast(r.area AS String)
    END AS Utf8
) AS area
FROM (
    SELECT area AS area
    FROM `test_results/analytics/github_issues_timeline`
    WHERE area IS NOT NULL
    UNION
    SELECT area AS area
    FROM `test_results/analytics/area_to_owner_mapping`
    UNION
    SELECT 'area/-' AS area
) AS r
WHERE r.area IS NOT NULL AND r.area != ''
ORDER BY area;
