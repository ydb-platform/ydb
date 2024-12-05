SELECT
    value,
    CASE
        WHEN value LIKE "abc" THEN "true"
        ELSE "false"
    END AS is_abc
FROM plato.Input
ORDER BY
    is_abc DESC,
    value ASC;
