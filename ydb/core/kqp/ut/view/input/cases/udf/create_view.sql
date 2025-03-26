CREATE VIEW `view_with_udf` WITH (security_invoker = TRUE) AS
    SELECT
        "bbb" LIKE Unwrap("aaa")
;
