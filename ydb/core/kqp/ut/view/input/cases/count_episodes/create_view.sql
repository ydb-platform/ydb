CREATE VIEW `/Root/count_episodes` WITH (security_invoker = TRUE) AS
    SELECT
        series_id,
        season_id,
        COUNT(*)
    FROM `/Root/episodes`
    GROUP BY
        series_id,
        season_id;
