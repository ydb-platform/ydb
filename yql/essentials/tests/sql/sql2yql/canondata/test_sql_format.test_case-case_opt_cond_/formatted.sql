SELECT
    CASE
        WHEN CAST('true' AS Bool) THEN 'FOO1'
        ELSE 'BAR1'
    END
UNION ALL
SELECT
    CASE
        WHEN CAST('false' AS Bool) THEN 'FOO2'
        ELSE 'BAR2'
    END
UNION ALL
SELECT
    CASE
        WHEN NULL THEN 'FOO3'
        ELSE 'BAR3'
    END
;
