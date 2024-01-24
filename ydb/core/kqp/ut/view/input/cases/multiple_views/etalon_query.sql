SELECT
    *
FROM (
    SELECT
        series.title AS series_title,
        seasons.title AS seasons_title
    FROM `/Root/view_series`
        AS series
    JOIN `/Root/view_seasons`
        AS seasons
    ON series.series_id == seasons.series_id
);
