SELECT
    *
FROM (
    SELECT
        series_id,
        season_id,
        COUNT(*)
    FROM `/Root/episodes`
    GROUP BY
        series_id,
        season_id
);
