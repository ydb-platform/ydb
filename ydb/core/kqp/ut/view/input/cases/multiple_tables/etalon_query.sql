SELECT
    *
FROM (
    SELECT
        *
    FROM `/Root/series`
        AS series
    JOIN (
        SELECT
            seasons.title AS seasons_title,
            episodes.title AS episodes_title,
            seasons.series_id AS series_id
        FROM `/Root/seasons`
            AS seasons
        JOIN `/Root/episodes`
            AS episodes
        ON seasons.series_id == episodes.series_id
    )
        AS seasons_and_episodes
    ON series.series_id == seasons_and_episodes.series_id
);
