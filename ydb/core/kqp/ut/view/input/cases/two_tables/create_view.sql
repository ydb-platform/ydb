CREATE VIEW `/Root/read_from_two_tables` WITH (security_invoker = TRUE) AS
    SELECT
        series.title AS series_title,
        seasons.title AS seasons_title
    FROM `/Root/series`
        AS series
    JOIN `/Root/seasons`
        AS seasons
    ON series.series_id == seasons.series_id;
