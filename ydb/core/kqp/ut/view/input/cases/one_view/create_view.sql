CREATE VIEW `/Root/read_from_one_view` WITH (security_invoker = TRUE) AS
    SELECT
        *
    FROM `/Root/view_series`;
