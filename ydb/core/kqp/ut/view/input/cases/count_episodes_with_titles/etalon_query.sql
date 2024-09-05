SELECT
    *
FROM (
    SELECT
        series.title AS series,
        seasons.title AS season,
        episodes.episode_count AS episode_count
    FROM (
        SELECT
            series_id,
            season_id,
            COUNT(*) AS episode_count
        FROM `/Root/episodes`
        GROUP BY
            series_id,
            season_id
    )
        AS episodes
    JOIN `/Root/series`
        AS series
    ON episodes.series_id == series.series_id
    JOIN `/Root/seasons`
        AS seasons
    ON episodes.series_id == seasons.series_id AND episodes.season_id == seasons.season_id
);
