SELECT
    *
FROM (
    SELECT
        series.title AS series_title,
        seasons.title AS seasons_title
    FROM view_series
        AS series
    JOIN view_seasons
        AS seasons
    ON series.series_id == seasons.series_id
);
