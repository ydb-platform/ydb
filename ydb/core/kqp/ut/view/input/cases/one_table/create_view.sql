CREATE VIEW read_from_one_table WITH (security_invoker = TRUE) AS
    SELECT
        *
    FROM series;
