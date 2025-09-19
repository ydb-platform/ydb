SELECT
    r.id,
    ROW_NUMBER() OVER w AS rank
FROM (
    SELECT 0 AS id
) AS r
JOIN (
    SELECT 0 AS id
) AS m
ON r.id = m.id
WINDOW w AS (
    PARTITION BY r.id
)
