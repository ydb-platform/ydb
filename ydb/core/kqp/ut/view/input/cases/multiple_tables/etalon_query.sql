SELECT
    *
FROM (
    SELECT
        *
    FROM series
    JOIN (
        SELECT
            seasons.title AS seasons_title,
            episodes.title AS episodes_title,
            seasons.series_id AS series_id
        FROM seasons
        JOIN episodes
        ON seasons.series_id == episodes.series_id
    )
        AS seasons_and_episodes
    ON series.series_id == seasons_and_episodes.series_id
);
