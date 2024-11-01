CREATE VIEW in_subquery WITH (security_invoker = TRUE) AS
    SELECT
        *
    FROM series
    WHERE series_id IN (
        SELECT
            series_id
        FROM series
    );
