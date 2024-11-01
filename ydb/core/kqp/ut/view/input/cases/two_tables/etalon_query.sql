SELECT
    *
FROM (
    SELECT
        series.title AS series_title,
        seasons.title AS seasons_title
    FROM `/Root/series`
        AS series
    JOIN `/Root/seasons`
        AS seasons
    ON series.series_id == seasons.series_id
) ORDER BY series_title, seasons_title;
