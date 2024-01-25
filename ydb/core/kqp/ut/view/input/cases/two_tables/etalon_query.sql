SELECT
    *
FROM (
    SELECT
        series.title AS series_title,
        seasons.title AS seasons_title
    FROM series
    JOIN seasons
    ON series.series_id == seasons.series_id
);
