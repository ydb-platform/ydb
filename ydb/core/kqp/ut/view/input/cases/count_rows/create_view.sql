CREATE VIEW `/Root/count_rows` WITH (security_invoker = TRUE) AS
    SELECT
        COUNT(*)
    FROM `/Root/episodes`;
