SELECT
    *
FROM (
    SELECT
        series.title AS series,
        series_stats.seasons_with_episode_count_greater_than_average AS seasons_with_episode_count_greater_than_average
    FROM (
        SELECT
            series_id,
            SUM(
                CASE
                    WHEN episode_count > average_episodes_in_season
                        THEN 1
                    ELSE 0
                END
            ) AS seasons_with_episode_count_greater_than_average
        FROM (
            SELECT
                series_id,
                season_id,
                episode_count,
                AVG(episode_count) OVER average_episodes_in_season_window AS average_episodes_in_season
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
            WINDOW
                average_episodes_in_season_window AS (
                    PARTITION BY
                        series_id
                )
        )
        GROUP BY
            series_id
    )
        AS series_stats
    JOIN `/Root/series`
        AS series
    USING (series_id)
);
