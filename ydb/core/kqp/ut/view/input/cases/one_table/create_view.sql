CREATE VIEW `/Root/read_from_one_table` WITH (security_invoker = TRUE) AS
    SELECT
        *
    FROM `/Root/series`;
