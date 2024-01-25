CREATE VIEW read_from_one_view WITH (security_invoker = TRUE) AS
    SELECT
        *
    FROM view_series;
